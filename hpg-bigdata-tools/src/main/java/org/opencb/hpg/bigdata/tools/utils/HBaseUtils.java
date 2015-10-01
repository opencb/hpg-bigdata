package org.opencb.hpg.bigdata.tools.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.opencb.commons.utils.CryptoUtils;

/**
 * Created by mh719 on 16/06/15.
 */
public class HBaseUtils {
    public static final int SV_THRESHOLD = 50; // TODO update as needed
    public static final String ROWKEY_SEPARATOR = "_";

    public static String buildRefernceStorageId(CharSequence chr, Long start, CharSequence refBases) {
        return buildStorageId(chr, start, refBases, refBases);
    }

    public static String buildStorageId(CharSequence chr, Long start, CharSequence refBases, CharSequence altBases) {
        StringBuilder builder = new StringBuilder();

        builder.append(buildStoragePosition(chr, start));

        builder.append(ROWKEY_SEPARATOR);

        if (refBases.length() < SV_THRESHOLD) {
            builder.append(refBases);
        } else {
            builder.append(new String(CryptoUtils.encryptSha1(refBases.toString())));
        }

        builder.append(ROWKEY_SEPARATOR);

        if (altBases.length() < SV_THRESHOLD) {
            builder.append(altBases);
        } else {
            builder.append(new String(CryptoUtils.encryptSha1(altBases.toString())));
        }

        return builder.toString();
    }

    public static String buildStoragePosition(CharSequence chr, Long pos) {
        String chrom = chr.toString();
        // check for chr at chromosome name and remove it (maybe expect it to be done before.
        if (chrom.length() > 2) {
            if (chrom.substring(0, 2).equals("chr")) {
                chrom = chrom.substring(2);
            }
        }
        if (chrom.length() < 2) {
            chrom = "0" + chrom;
        }

        StringBuilder builder = new StringBuilder();
        builder.append(chrom);
        builder.append(ROWKEY_SEPARATOR);
        builder.append(String.format("%012d", pos));
        return builder.toString();
    }

    /**
     * Create default HBase table layout with one column family using {@link COLUMN_FAMILY}.
     *
     * @param tablename     HBase table name
     * @param columnFamily  Column Family
     * @param configuration HBase configuration
     * @return boolean True if a new table was created
     * @throws IOException throws {@link IOException} from creating a connection / table
     **/
    public static boolean createTableIfNeeded(String tablename, byte[] columnFamily, Configuration configuration) throws IOException {
        TableName tname = TableName.valueOf(tablename);
        try (
                Connection con = ConnectionFactory.createConnection(configuration);
                Table table = con.getTable(tname);
                Admin admin = con.getAdmin();
        ) {
            if (!exist(tname, admin)) {
                HTableDescriptor descr = new HTableDescriptor(tname);
                descr.addFamily(new HColumnDescriptor(columnFamily));
                admin.createTable(descr);
                return true;
            }
        }
        return false;
    }

    public static boolean exist(TableName tname, Admin admin) throws IOException {
        for (TableName tn : admin.listTableNames()) {
            if (tn.equals(tname)) {
                return true;
            }
        }
        return false;
    }
}
