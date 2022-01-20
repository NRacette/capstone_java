package org.example;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;

public class App
{
    public static void main( String[] args )
    {
        DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        pipelineOptions.setJobName("nrrJava");
        pipelineOptions.setProject("york-cdf-start");
        pipelineOptions.setRegion("us-central1");
        pipelineOptions.setRunner(DataflowRunner.class);
        pipelineOptions.setGcpTempLocation("gs://york-nrr//temp");


        Pipeline pipeline = Pipeline.create(pipelineOptions);

        TableSchema tb1 = new TableSchema().setFields(Arrays.asList(
                        new TableFieldSchema().setName("cust_tier_code").setType("STRING").setMode("REQUIRED"),
                        new TableFieldSchema().setName("sku").setType("INTEGER").setMode("REQUIRED"),
                        new TableFieldSchema().setName("total_no_of_product_views").setType("INTEGER").setMode("REQUIRED")
                        )
                );

        TableSchema tb2 = new TableSchema().setFields(Arrays.asList(
                        new TableFieldSchema().setName("cust_tier_code").setType("STRING").setMode("REQUIRED"),
                        new TableFieldSchema().setName("sku").setType("INTEGER").setMode("REQUIRED"),
                        new TableFieldSchema().setName("total_sales_amount").setType("INTEGER").setMode("REQUIRED")
                        )
                );

        PCollection<TableRow> ViewsTable =
                pipeline
                        .apply(
                                "Read in views table",
                                BigQueryIO.readTableRows()
                                        .fromQuery("SELECT c.CUST_TIER_CODE as cust_tier_code, p.SKU as sku, COUNT(*) as total_no_of_product_views " +
                                                "FROM `york-cdf-start.final_input_data.customers` as c " +
                                                "JOIN `york-cdf-start.final_input_data.product_views` as p ON c.CUSTOMER_ID = p.CUSTOMER_ID " +
                                                "GROUP BY sku, cust_tier_code;").usingStandardSql());

        PCollection<TableRow> SalesTable =
                pipeline
                        .apply(
                                "Read in sales table",
                                BigQueryIO.readTableRows()
                                        .fromQuery("SELECT c.CUST_TIER_CODE as cust_tier_code, o.SKU as sku, SUM(o.ORDER_AMT) as total_sales_amount" +
                                                "FROM `york-cdf-start.final_input_data.customers` as c " +
                                                "JOIN `york-cdf-start.final_input_data.orders` as o ON c.CUSTOMER_ID = o.CUSTOMER_ID " +
                                                "GROUP BY sku, cust_tier_code;").usingStandardSql());
        ViewsTable.apply(
                BigQueryIO.<TableRow>write()
                        .to("york-cdf-start.final_nick_racette_java.cust_tier_code-sku-total_no_of_product_views")
                        .withSchema(tb1)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

        SalesTable.apply(
                BigQueryIO.<TableRow>write()
                        .to("york-cdf-start.final_nick_racette_java.cust_tier_code-sku-total_sales_amount")
                        .withSchema(tb2)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));


        pipeline.run().waitUntilFinish();

    }
}
