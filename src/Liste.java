import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Liste {

    public static class Fichier {
        private final Text key;
        private final IntWritable value;

        public Fichier(Text key, IntWritable value) {
            this.key = key;
            this.value = value;
        }

        public Text getKey() {
            return key;
        }
        public IntWritable getValue() {
            return value;
        }
    }

    // this is to check whether the connection is established with the database(in this case, phpmyadmin with the assistance of xampp server)
    public static Connection getConnection() {

        Connection connection = null;
        try {

            Class.forName("com.mysql.jdbc.Driver");
            System.out.println("Driver Loaded");
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/hadoop_database","root","");
            System.out.println("Connected...");

            //Checks if table "fichier" exists, if not it creates it
            DatabaseMetaData dbm = connection.getMetaData();
            ResultSet tables = dbm.getTables(null, null, "fichier", null);
            if (!tables.next()) {
                String tableName = "fichier";

                String column1 = "id";
                String column1Type = "int";

                String column2 = "key_f";
                String column2Type = "varchar(50)";

                String column3 = "value_f";
                String column3Type = "int";

                String column4 = "source";
                String column4Type = "int";

                Statement stmt = connection.createStatement();
                String query = "create table " + tableName + " ( " + column1+" " + column1Type + " PRIMARY KEY NOT NULL AUTO_INCREMENT, " +
                        column2 +" " + column2Type +  " , " +
                        column3 +" " + column3Type +  " , " +
                        column4 +" " + column4Type + " )";

                System.out.println(query);
                stmt.executeUpdate(query);
                stmt.close();
            }

        } catch(Exception e) {
            e.printStackTrace();
        }

        return connection;
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }

        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();
        public ArrayList<Fichier> synthese = new ArrayList<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            //Connecting to db
            Connection connection = getConnection();
            Statement stmt = null;
            try { stmt = connection.createStatement(); } catch (SQLException throwables) { throwables.printStackTrace(); }

            Configuration conf = context.getConfiguration();
            int source =Integer.parseInt(conf.get("source"));

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);

            Fichier obj = new Fichier(key,result);
            synthese.add(obj);

            //Deleting duplicate
            List<Fichier> noRepeat = new ArrayList<>();

            for (Fichier fichier : synthese) {
                boolean isFound = false;
                // check if the event name exists in noRepeat
                for (Fichier e : noRepeat) {
                    if (e.getKey().equals(fichier.getKey()) || (e.equals(fichier))) {
                        isFound = true;
                        break;
                    }
                }
                if (!isFound) noRepeat.add(fichier);
            }

            for (Fichier fichier : noRepeat) {
                System.out.println("___________ "+fichier.getKey());

                String query = "INSERT INTO Fichier ( key_f, value_f, source) " +
                        "VALUES ('"+fichier.getKey() +"',"+ fichier.getValue()+","+ source+");";
                System.out.println(query);

                try {
                    stmt.executeUpdate(query); } catch (SQLException throwables) { throwables.printStackTrace(); }
                try { stmt.close(); } catch (SQLException throwables) { throwables.printStackTrace(); }

                context.write(fichier.getKey(), fichier.getValue());
            }

            BufferedWriter writer = new BufferedWriter(new FileWriter("errors.txt", true));

            if (source == 0) {

                for (Fichier fichier : noRepeat) {
                    String str = fichier.getKey().toString();
                    String[] arrOfStr = str.split("\\.", -1);

                    try {

                        //fire ArrayIndexOutOfBoundsException if not file first
                        if (arrOfStr[1].length() ==0 ) {
                            continue;
                        }

                        if (arrOfStr[0].length() != 5) {
                            String msg = key + "  : erreur dans le nom du fichier (longueur != 5 charactères). \n";
                            writer.write(msg);
                        }

                        if (arrOfStr[1].length() != 3) {
                            String msg = key + "  : erreur dans l'extension du fichier (longueur != 3 charactères). \n ";
                            writer.write(msg);
                        }

                        //Checks is file name is present more than one time in Lnom
                        if (!result.equals(new IntWritable(1))) {
                            String msg = key + "   est présent plusq'une fois dans Lnom ( exactement " + result + " fois ) \n";
                            writer.write(msg);
                        }
                    }
                    catch(ArrayIndexOutOfBoundsException exception) {
                        String msg = key + "  n'est pas un fichier. \n";
                        writer.write(msg);
                    }
                }
                writer.write("------------------------------------------ \n");

                writer.close();
            }
        }
    }
    public static void getMatches() throws SQLException, IOException {

        ArrayList<String> lnom_file_names = new ArrayList<>();
        ArrayList<Fichier> lists_files_final = new ArrayList<>();

        ArrayList<Fichier> matches_list = new ArrayList<>();
        ArrayList<Fichier> no_matches_list = new ArrayList<>();
        ArrayList<Fichier> noRepeat = new ArrayList<>();

        Connection connection = getConnection();
        Statement stmt = connection.createStatement();
        Statement stmt2 = connection.createStatement();
        ResultSet lnom_files = stmt.executeQuery("SELECT * FROM Fichier WHERE source = 0");
        ResultSet lists_files = stmt2.executeQuery("SELECT * FROM Fichier WHERE source = 1");

        while (lnom_files.next()) {

            String key = lnom_files.getString("key_f");
            lnom_file_names.add(key);
        }

        while (lists_files.next()) {

            String key = lists_files.getString("key_f");
            String value = lists_files.getString("value_f");
            int value_int = Integer.parseInt(value);

            lists_files_final.add(new Fichier(new Text(key),new IntWritable(value_int)));
        }

        for (Fichier fichier : lists_files_final) {
            for (String key : lnom_file_names) {
                if (((fichier.getKey()).toString().equals(key))){
                    matches_list.add(new Fichier(fichier.getKey(), fichier.getValue()));
                }else{
                    no_matches_list.add(new Fichier(new Text(key), new IntWritable(0)));

                    //Deleting duplicate
                    for (Fichier fich : no_matches_list) {
                        boolean isFound = false;
                        // check if the event name exists in noRepeat
                        for (Fichier e : noRepeat) {
                            if (e.getKey().equals(fich.getKey()) || (e.equals(fich))) {
                                isFound = true;
                                break;
                            }
                        }
                        if (!isFound) noRepeat.add(fich);
                    }
                }
            }
        }

        matches_list.addAll(noRepeat);
        ArrayList<Fichier> final_list = new ArrayList<>();

        //Deleting duplicate
        for (Fichier fich : matches_list) {
            boolean isFound = false;
            // check if the event name exists in noRepeat
            for (Fichier e : final_list) {
                if (e.getKey().equals(fich.getKey()) || (e.equals(fich))) {
                    isFound = true;
                    break;
                }
            }
            if (!isFound) final_list.add(fich);
        }

        //Set true for append mode
        BufferedWriter writer = new BufferedWriter(new FileWriter("result.txt", true));
        String first_line = "*** Résultat de la recherche *** \n \n";
        writer.write(first_line);

        for (Fichier fichier : final_list) {
            System.out.println(fichier.getKey() + " "+ fichier.getValue());
            String textToAppend = ""+fichier.getKey() + " "+ fichier.getValue()+" \n";
            writer.write(textToAppend);
            writer.write("------------------------------------------ \n");
        }
        writer.close();
        System.out.println("________________ end");
    }

    public static void main(String[] args) throws Exception {

        Connection connection = getConnection();
        Statement stmt = null;
        try { stmt = connection.createStatement(); } catch (SQLException throwables) { throwables.printStackTrace(); }
        //Checks if table "fichier" exists, if not it creates it
        DatabaseMetaData dbm = connection.getMetaData();
        ResultSet tables = dbm.getTables(null, null, "fichier", null);
        if (tables.next()) {
            String query = "DROP TABLE Fichier" ;
            System.out.println(query);

            try { stmt.executeUpdate(query); } catch (SQLException throwables) { throwables.printStackTrace(); }
            try { stmt.close(); } catch (SQLException throwables) { throwables.printStackTrace(); }
        }

        //running two jobs
        for (int i = 0; i <2; i++) {

            Configuration conf = new Configuration();
            //Variable referring to 0 if the input is Lnom and to 1 if the input is five_lists
            String source = Integer.toString(i);
            conf.set("source", source);

            Job job = Job.getInstance(conf, "word count");
            job.setJarByClass(Liste.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[i]));
            FileOutputFormat.setOutputPath(job, new Path(args[i+2]));
            job.waitForCompletion(true);
        }

        getMatches();
    }
}