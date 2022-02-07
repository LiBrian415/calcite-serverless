import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.tpcds.TpcdsSchema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;

import java.nio.file.Files;
import java.nio.file.Path;

import static java.util.Objects.requireNonNull;

/**
 * "Stateless" Planner
 *   Pipeline: Syntax Analysis -> Semantic Analysis -> AST to Rel. -> Optimization
 *   (See PlannerImpl.java for reference)
 */
public class Optimizer {

    public static void main(String[] args) throws Exception {

        // Expects a file containing a tpc-ds sql query
        String sql = Files.readString(Path.of(args[0]));

        FrameworkConfig config = createConfig();

        // 1. Parse (parse())
        SqlParser.Config parserConfig = config.getParserConfig();
        SqlParser parser = SqlParser.create(sql, parserConfig);
        SqlNode sqlNode = parser.parseStmt();

        // 2. Validate (validate())
        SqlValidator validator = createSqlValidator(config);
        SqlNode validatedSqlNode = validator.validate(sqlNode);

        // 3. Rel (rel())
        final RelOptPlanner planner = new VolcanoPlanner(
                config.getCostFactory(),
                config.getContext()
        );
        RelOptUtil.registerDefaultRules(
                planner,
                connConfig(config.getContext(), config.getParserConfig()).materializationsEnabled(),
                Hook.ENABLE_BINDABLE.get(false));
        planner.setExecutor(config.getExecutor());

        if (config.getTraitDefs() == null) {
            planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
            if (CalciteSystemProperty.ENABLE_COLLATION_TRAIT.value()) {
                planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
            }
        } else {
            for (RelTraitDef def : config.getTraitDefs()) {
                planner.addRelTraitDef(def);
            }
        }

        final RelOptCluster cluster = RelOptCluster.create(
                requireNonNull(planner, "planner"),
                new RexBuilder(new JavaTypeFactoryImpl(config.getTypeSystem())));

        final SqlToRelConverter.Config converterConfig = config.getSqlToRelConverterConfig()
                .withTrimUnusedFields(false);

        final SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(
                null,
                validator,
                createCatalogReader(config),
                cluster,
                config.getConvertletTable(),
                converterConfig);

        RelRoot root = sqlToRelConverter.convertQuery(validatedSqlNode, false, true);

        // 3.5. Transformation
        root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
        final RelBuilder relBuilder = converterConfig.getRelBuilderFactory().create(cluster, null);
        root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder));

        // 4. Optimization
        Program program = config.getPrograms().get(0);
        RelNode transformed = program.run(
                requireNonNull(planner, "planner"),
                root.rel,
                root.rel.getTraitSet().replace(EnumerableConvention.INSTANCE),
                ImmutableList.of(),
                ImmutableList.of());

        System.out.println(RelOptUtil.toString(transformed));
    }

    private static FrameworkConfig createConfig() {
        final TpcdsSchema tpcds = new TpcdsSchema(1);
        final SchemaPlus schema = Frameworks.createRootSchema(true);
        schema.add("tpcds", tpcds);

        return Frameworks.newConfigBuilder()
                .defaultSchema(schema)
                .parserConfig(SqlParser.config()
                        .withCaseSensitive(false)
                        .withQuotedCasing(Casing.UNCHANGED)
                        .withUnquotedCasing(Casing.UNCHANGED))
                .programs(Programs.ofRules(Programs.RULE_SET))
                .build();
    }

    private static CalciteCatalogReader createCatalogReader(FrameworkConfig config) {
        SchemaPlus defaultSchema = requireNonNull(config.getDefaultSchema(), "defaultSchema");
        final SchemaPlus rootSchema = rootSchema(defaultSchema);
        return new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                ImmutableList.of("tpcds"), // Used to avoid needed to prepend "tpcds" to tables in the query
                new JavaTypeFactoryImpl(config.getTypeSystem()),
                connConfig(config.getContext(), config.getParserConfig())
        );
    }

    private static SqlValidator createSqlValidator(FrameworkConfig config) {
        final CalciteCatalogReader catalogReader = createCatalogReader(config);
        final SqlOperatorTable opTab =
                SqlOperatorTables.chain(config.getOperatorTable(), catalogReader);
        final CalciteConnectionConfig connConfig = connConfig(config.getContext(), config.getParserConfig());
        return SqlValidatorUtil.newValidator(
                opTab,
                catalogReader,
                new JavaTypeFactoryImpl(config.getTypeSystem()),
                SqlValidator.Config.DEFAULT
                        .withLenientOperatorLookup(connConfig.lenientOperatorLookup())
                        .withSqlConformance(connConfig.conformance())
                        .withDefaultNullCollation(connConfig.defaultNullCollation())
                        .withIdentifierExpansion(true));
    }

    private static SchemaPlus rootSchema(SchemaPlus schema) {
        for (;;) {
            SchemaPlus parent = schema.getParentSchema();
            if (parent == null) {
                return schema;
            }
            schema = parent;
        }
    }

    /** Gets a user-defined config and appends default connection values. */
    private static CalciteConnectionConfig connConfig(Context context,
            SqlParser.Config parserConfig) {
        CalciteConnectionConfigImpl config =
                context.maybeUnwrap(CalciteConnectionConfigImpl.class)
                        .orElse(CalciteConnectionConfig.DEFAULT);
        if (!config.isSet(CalciteConnectionProperty.CASE_SENSITIVE)) {
            config = config.set(CalciteConnectionProperty.CASE_SENSITIVE,
                    String.valueOf(parserConfig.caseSensitive()));
        }
        if (!config.isSet(CalciteConnectionProperty.CONFORMANCE)) {
            config = config.set(CalciteConnectionProperty.CONFORMANCE,
                    String.valueOf(parserConfig.conformance()));
        }
        return config;
    }
}
