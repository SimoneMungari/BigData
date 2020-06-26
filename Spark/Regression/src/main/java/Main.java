import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.*;
import org.apache.spark.mllib.tree.GradientBoostedTrees;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Timestamp;

public class Main {

    public static void main(String[] args) {
        //local
        //SparkSession spark = SparkSession.builder().config("spark.master","local").appName("Regression").getOrCreate();


        //cluster
        SparkSession spark = SparkSession.builder().appName("Regression").getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        System.out.println("Session created");

        Dataset<Row> sales_train_validation_original = spark.read().option("maxColumns", 35000).option("header", "true").option("delimiter", ";").format("csv").load(args[0]).toDF();

        RandomForestRegressor rf = new RandomForestRegressor().setFeaturesCol("features")
                .setNumTrees(50)
                .setMaxDepth(15)
                .setFeatureSubsetStrategy(RandomForestRegressor.supportedFeatureSubsetStrategies()[1]);

        RandomForestRegressionModel model = null;

        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setPredictionCol("prediction")
                .setMetricName("rmse");

        sales_train_validation_original = sales_train_validation_original.withColumn("wday", sales_train_validation_original.col("wday").cast(DataTypes.IntegerType));
        sales_train_validation_original = sales_train_validation_original.withColumn("event_type_1", sales_train_validation_original.col("event_type_1").cast(DataTypes.LongType));
        sales_train_validation_original = sales_train_validation_original.withColumn("event_type_2", sales_train_validation_original.col("event_type_2").cast(DataTypes.LongType));
        sales_train_validation_original = sales_train_validation_original.withColumn("month", sales_train_validation_original.col("month").cast(DataTypes.IntegerType));
        sales_train_validation_original = sales_train_validation_original.withColumn("year", sales_train_validation_original.col("year").cast(DataTypes.IntegerType));

        VectorAssembler assembler = new VectorAssembler().setInputCols(new String[]{"wday", "event_type_1", "event_type_2", "month", "year"}).setOutputCol("features");
        sales_train_validation_original = assembler.transform(sales_train_validation_original);

        Dataset<Row> sales_train_validation_submit_original = sales_train_validation_original.where("Day > 1913");
        sales_train_validation_original = sales_train_validation_original.where("Day <= 1913");

        sales_train_validation_original = sales_train_validation_original.cache();
        sales_train_validation_submit_original = sales_train_validation_submit_original.cache();

        String[] colNames = sales_train_validation_original.columns();

        //i = 6 because the columns with index 0-5 come from the join with calendar, columns of products start from index 6
        for(int i = 6; i < colNames.length; i++) {
            String colName = colNames[i];
            Dataset<Row> sales_train_validation = sales_train_validation_original.select("features", colName);
            Dataset<Row> sales_train_validation_submit = sales_train_validation_submit_original.select("features", colName);

            sales_train_validation = sales_train_validation.withColumn(colName, sales_train_validation.col(colName).cast(DataTypes.IntegerType));
            sales_train_validation_submit = sales_train_validation_submit.withColumn(colName, sales_train_validation_submit.col(colName).cast(DataTypes.IntegerType));

            rf.setLabelCol(colName);
            evaluator.setLabelCol(colName);

            model = rf.fit(sales_train_validation);
            Dataset<Row> result = model.transform(sales_train_validation_submit);

            double rmse = evaluator.evaluate(result);
            result.select("prediction").coalesce(1).write().format("com.databricks.spark.csv").option("header", "true").save(colName);
        }

        spark.stop();
    }


}
