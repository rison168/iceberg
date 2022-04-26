package com.rison.iceberg.flink.schema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @PACKAGE_NAME: com.rison.iceberg.flink.schema
 * @NAME: ReadIcebergSchema
 * @USER: Rison
 * @DATE: 2022/4/26 15:17
 * @PROJECT_NAME: iceberg
 **/
public class ReadIcebergSchema {
    public static Configuration getHadoopConfig() throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hdfsCluster");
        conf.addResource(new Path("/usr/hdp/current/hadoop-client/etc/hadoop/hdfs-site.xml"));
        conf.addResource(new Path("/usr/hdp/current/hadoop-client/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/hdp/current/hive-client/conf/hive-site.xml"));
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromSubject(null);
        return conf;
    }

    private static TableLoader getTableLoad(String catalog, String db, String table, String catalogUri, String catalogWarehouseLocation) {
        try {
            Map<String, String> catalogProperties = new HashMap<>();
            catalogProperties.put("type", "iceberg");
            catalogProperties.put(FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, FlinkCatalogFactory.ICEBERG_CATALOG_TYPE_HIVE);
            catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, catalogWarehouseLocation);
            catalogProperties.put(CatalogProperties.URI, catalogUri);
            CatalogLoader catalogLoader = CatalogLoader.hive(catalog, getHadoopConfig(), catalogProperties);

            TableIdentifier identifier = TableIdentifier.of(Namespace.of(db), table);
            Catalog catalogObj = catalogLoader.loadCatalog();
            if (!catalogObj.tableExists(identifier)) {
                System.out.println("getTableLoad(" + catalog + "," + db + "," + table + ") not exists :");
                return null;
            }
            TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, identifier);
            return tableLoader;
        } catch (Exception e) {
            System.out.println("getTableLoad(" + catalog + "," + db + "," + table + ") exception :" + e.toString());
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) {

        TableLoader tableLoader = getTableLoad("hive_catalog", "flinkuser", "test_type_tbl", "thrift://tbds-172-16-16-41:9083", "hdfs:///apps/hive/warehouse");
        tableLoader.open();
        Table table = tableLoader.loadTable();
        TableOperations operations = ((BaseTable) table).operations();
        TableMetadata metadata = operations.current();
        operations.commit(metadata, metadata.upgradeToFormatVersion(2));

        Schema schema = table.schema();
        List<Types.NestedField> columns = schema.columns();
        for (Types.NestedField t : columns){
            System.out.println( ">>>>> " + t + t.type() + ">>" + t.type().toString());
        }
        System.out.println("===========================");
        System.out.println("DateType " + Types.DateType.get() + " >> " + Types.DateType.get().toString());
        System.out.println("TimeType " + Types.TimeType.get() + " >> " + Types.TimeType.get().toString());
        System.out.println("IntegerType " + Types.IntegerType.get() + " >> " + Types.IntegerType.get().toString());
        System.out.println("BinaryType " + Types.BinaryType.get() + " >> " + Types.BinaryType.get().toString());
        System.out.println("BooleanType " + Types.BooleanType.get() + " >> " + Types.BooleanType.get().toString());
        System.out.println("FloatType " + Types.FloatType.get() + " >> " + Types.FloatType.get().toString());
        System.out.println("LongType " + Types.LongType.get() + " >> " + Types.LongType.get().toString());
        System.out.println("DoubleType " + Types.DoubleType.get() + " >> " + Types.DoubleType.get().toString());
        System.out.println("DecimalType " + Types.DecimalType.of(10, 2) + " >> " + Types.DecimalType.of(10, 2).toString());

    }
}
