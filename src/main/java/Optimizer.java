import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.tpcds.TpcdsSchema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSets;

import java.util.Collections;
import java.util.Properties;

public class Optimizer {

    /**
     * Pipeline: Syntax Analysis -> Semantic Analysis -> AST to Rel. -> Optimization
     * (See PlannerImpl.java for reference)
     */
    public static void main(String[] args) throws Exception {

        String sql = "SELECT * FROM CALL_CENTER";

        // Create Schema for TPC-DS
        String schemaName = "tpcds";
        TpcdsSchema tpcds = new TpcdsSchema(1);

        // Config
        Properties configProperties = new Properties();
        configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        configProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        configProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(configProperties);

        // 1. Parse
        SqlParser.Config parserConfig = SqlParser.config()
                .withCaseSensitive(config.caseSensitive())
                .withUnquotedCasing(config.unquotedCasing())
                .withQuotedCasing(config.quotedCasing())
                .withConformance(config.conformance());
        SqlParser parser = SqlParser.create(sql, parserConfig);
        SqlNode sqlNode = parser.parseStmt();

        // 2. Validate
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        rootSchema.add(schemaName, tpcds);

        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
                rootSchema,
                Collections.singletonList(schemaName),
                typeFactory, config
        );
        SqlOperatorTable operatorTable = SqlOperatorTables.chain(SqlStdOperatorTable.instance(), catalogReader);
        SqlValidator.Config sqlValidatorConfig = SqlValidator.Config.DEFAULT
                .withLenientOperatorLookup(config.lenientOperatorLookup())
                .withSqlConformance(config.conformance())
                .withDefaultNullCollation(config.defaultNullCollation())
                .withIdentifierExpansion(true);
        SqlValidator validator = SqlValidatorUtil.newValidator(
                operatorTable,
                catalogReader,
                typeFactory,
                sqlValidatorConfig);
        SqlNode validatedSqlNode = validator.validate(sqlNode);

        // 3. Rel
        RelOptPlanner planner = new VolcanoPlanner(
                RelOptCostImpl.FACTORY,
                Contexts.of(config));
        RelOptUtil.registerDefaultRules(planner,
                config.materializationsEnabled(),
                Hook.ENABLE_BINDABLE.get(false));
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        RelOptCluster cluster = RelOptCluster.create(
                planner,
                new RexBuilder(typeFactory));

        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(false);

        SqlToRelConverter converter = new SqlToRelConverter(
                null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig);

        RelNode root = converter.convertQuery(validatedSqlNode, false, true).rel;

        // 4. Optimization
        Program program = Programs.of(RuleSets.ofList(planner.getRules()));
        RelNode transformed = program.run(
                planner,
                root,
                root.getTraitSet().replace(EnumerableConvention.INSTANCE),
                Collections.emptyList(),
                Collections.emptyList());
    }

}
