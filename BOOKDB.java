/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ceng.ceng351.bookdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 *
 * @author sahin kasap
 */
public class BOOKDB implements IBOOKDB{
    private static String user = "2264562"; // TODO: Your userName
    private static String password = "dd9e223b"; //  TODO: Your password
    private static String host = "144.122.71.65"; // host name
    private static String database = "db2264562"; // TODO: Your database name
    private static int port = 8084; // port

    private static Connection connection = null;


    @Override
    public void initialize() {
        String url = "jdbc:mysql://" + host + ":" + port + "/" + database;

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return;
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int createTables() {
        int result;
        int numberofTablesInserted = 0;


        //Player(number:integer, teamname:char(20), name:char(30), age:integer, position:char(3))
        String queryCreateAuthorTable = "create table if not exists author (" + 
                                                                   "author_id int ," + 
                                                                   "author_name varchar(60) ," + 
                                                                   "primary key (author_id));";
        String queryCreatePublisherTable = "create table if not exists publisher (" + 
                                                                   "publisher_id int ," + 
                                                                   "publisher_name varchar(50) ," + 
                                                                   "primary key (publisher_id));";
        String queryCreateBookTable = "create table if not exists book (" + 
                                                                   "isbn char(13)," + 
                                                                   "book_name varchar(120) ," + 
                                                                   "publisher_id int," + 
                                                                   "first_publish_year char(4) ," + 
                                                                   "page_count int ," + 
                                                                   "category varchar(25) ," + 
                                                                   "rating float ," + 
                                                                   "primary key (isbn)," +
                                                                   "FOREIGN KEY (publisher_id) REFERENCES publisher(publisher_id));";
        String queryCreateAuthorOfTable = "create table if not exists author_of (" + 
                                                                   "isbn char(13)," + 
                                                                   "author_id int," +
                                                                   "primary key (isbn,author_id)," +
                                                                   "FOREIGN KEY (isbn) REFERENCES book(isbn)," +
                                                                   "FOREIGN KEY (author_id) REFERENCES author(author_id));";
        String queryCreatePhw1Table = "create table if not exists phw1 (" + 
                                                                   "isbn char(13)," + 
                                                                   "book_name varchar(120) ," + 
                                                                   "rating float );" ;
        
        try {
            Statement statement = this.connection.createStatement();

            //Player Table
            result = statement.executeUpdate(queryCreateAuthorTable);
            //System.out.println(result); //TODO
            numberofTablesInserted++;
            result = statement.executeUpdate(queryCreatePublisherTable);
            //System.out.println(result);
            numberofTablesInserted++;
            result = statement.executeUpdate(queryCreateBookTable);
            //System.out.println(result);
            numberofTablesInserted++;
            result = statement.executeUpdate(queryCreateAuthorOfTable);
          //  System.out.println(result);
            numberofTablesInserted++;
            result = statement.executeUpdate(queryCreatePhw1Table);
          //  System.out.println(result);
            numberofTablesInserted++;
            

            //close
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return numberofTablesInserted;
    }

    @Override
    public int dropTables() {
        String queryDropAuthorTable = "DROP TABLE if exists author;" ;
        String queryDropPublisherTable = "DROP TABLE if exists publisher;";
        String queryDropBookTable = "DROP TABLE if exists book;";
        String queryDropAuthorOfTable = "DROP TABLE if exists author_of;";
        String queryDropPhw1Table = "DROP TABLE if exists phw1;";
        try {
            Statement statement = this.connection.createStatement();

            //Player Table
            statement.executeUpdate(queryDropAuthorOfTable);
            
            statement.executeUpdate(queryDropBookTable);
            
            statement.executeUpdate(queryDropAuthorTable);
            
            statement.executeUpdate(queryDropPublisherTable);
            
            statement.executeUpdate(queryDropPhw1Table);
           
            

            //close
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 5;
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    int inserta(Author a){
        //TODO
        String b = "INSERT INTO author (author_id,author_name) VALUES (?,?);";
        try {
            PreparedStatement statement = this.connection.prepareStatement(b);
            statement.setInt(1, a.getAuthor_id());
            statement.setString(2,a.getAuthor_name());
            int aa = statement.executeUpdate();
            statement.close();
            return aa;
            //close
            
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public int insertAuthor(Author[] authors) {
        int l = authors.length;
        int res=0;
        for(int i=0;i<l;i++){
            res += inserta(authors[i]);
        }
        return res;
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    int insertb(Book bo){
        String b = "INSERT INTO book (isbn ,book_name , publisher_id, first_publish_year ,page_count"
                + ",category,rating) VALUES (?,?,?,?,?,?,?);";//TODO 
        try {
            PreparedStatement statement = this.connection.prepareStatement(b);
            statement.setString(1, bo.getIsbn());
            statement.setString(2, bo.getBook_name());
            statement.setInt(3, bo.getPublisher_id());
            statement.setString(4, bo.getFirst_publish_year());
            statement.setInt(5, bo.getPage_count());
            statement.setString(6, bo.getCategory());
            statement.setDouble(7, bo.getRating());
            int a = statement.executeUpdate();
            statement.close();     //close
            return a;
            
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }
    @Override
    public int insertBook(Book[] books) {
        int l = books.length;
        int b=0;
        for(int i=0;i<l;i++){
            b+=insertb(books[i]);
        }
        return b;
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    /*    String queryCreatePublisherTable = "create table if not exists publisher (" + "publisher_id int ," + "publisher_name varchar(50) ," + 
                                                                   "primary key (publisher_id));";   */
    int insertp(Publisher p){
        String pu = "INSERT INTO publisher(publisher_id,publisher_name) VALUES(?,?);";
        int res=0;
        try {
            PreparedStatement statement = this.connection.prepareStatement(pu);
            statement.setInt(1, p.getPublisher_id());
            statement.setString(2, p.getPublisher_name());
            res = statement.executeUpdate();
            statement.close();     //close
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return res;
    }
    @Override
    public int insertPublisher(Publisher[] publishers) {
        int l = publishers.length;
        int res=0;
        for(int i=0;i<l;i++)
            res += insertp(publishers[i]);
        return res;
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    /*author_of (isbn char(13),author_id int," +*/
    int insertao(Author_of ao){
        int res=0;
        
        String a = "INSERT INTO author_of VALUES(?,?);";
        try {
            PreparedStatement statement = this.connection.prepareStatement(a);
            statement.setString(1, ao.getIsbn());
            statement.setInt(2, ao.getAuthor_id());
            res = statement.executeUpdate();
            statement.close();     //close
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return res;
    }
    @Override
    public int insertAuthor_of(Author_of[] author_ofs) {
        int l=author_ofs.length;
        int res=0;
        for(int i=0;i<l;i++){
            res += insertao(author_ofs[i]);
        }
        return res;
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public QueryResult.ResultQ1[] functionQ1() {
    /*  List isbn, first publish year, page count and publisher name of the book(s) which have the maximum number of pages.
        Order the results by isbn in ascending order*/
        QueryResult.ResultQ1[] res=null;
        ArrayList<QueryResult.ResultQ1> re=new ArrayList<QueryResult.ResultQ1>();
        String q = "SELECT b.isbn,b.first_publish_year,p.publisher_id,p.publisher_name FROM book b, publisher p WHERE p.publisher_id = b.publisher_id AND b.page_count = (SELECT max(bo.page_count) FROM book bo ) ORDER BY b.isbn ASC";
        try {
            Statement s = this.connection.createStatement();
            ResultSet r = s.executeQuery(q);
            while(r.next()){
                String is = r.getString("isbn");
                String fpy= r.getString("first_publish_year");
                int pc= r.getInt("publisher_id");
                String pn=r.getString("publisher_name");
                QueryResult.ResultQ1 qq = new QueryResult.ResultQ1(is,fpy,pc,pn);
                re.add(qq);
                
                
            }
            res=re.toArray(new QueryResult.ResultQ1[0]);
            s.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return res;
    }

    @Override
    public QueryResult.ResultQ2[] functionQ2(int author_id1, int author_id2) {
        QueryResult.ResultQ2[] res =null;
        String q = "SELECT  bo.publisher_id,AVG(page_count) as page_count FROM book bo where bo.publisher_id = (SELECT b.publisher_id from book b, author_of ao,author_of ao2 where ao.isbn=b.isbn AND ao .author_id=" + author_id1 
                +" AND ao2.author_id="+ author_id2 +" AND ao2.isbn=ao.isbn) group by bo.publisher_id order by publisher_id asc;";
        ArrayList<QueryResult.ResultQ2> re=new ArrayList<QueryResult.ResultQ2>();
         try {
            Statement s = this.connection.createStatement();
            ResultSet r = s.executeQuery(q);
            while(r.next()){                
                int pc = r.getInt("publisher_id");
                double pcount= r.getDouble("page_count");
                QueryResult.ResultQ2 qq = new QueryResult.ResultQ2(pc,pcount);
                re.add(qq);
            }
            res=re.toArray(new QueryResult.ResultQ2[0]);
            s.close();            
        } catch (Exception e) {
            e.printStackTrace();
        }        
        return res;       
    }

    @Override
    public QueryResult.ResultQ3[] functionQ3(String author_name) {
        String s="SELECT boo.book_name, boo.category, MIN(boo.first_publish_year) as fpy From book boo,author_of aoo where boo.isbn =aoo.isbn AND aoo.author_id=( SELECT author_id from author a where a.author_name=?) GROUP BY boo.isbn ORDER BY boo.book_name,boo.category,boo.first_publish_year asc ;";
        QueryResult.ResultQ3[] res=null;
        ArrayList<QueryResult.ResultQ3> re=new ArrayList<QueryResult.ResultQ3>();
        ResultSet r;
        try{
            PreparedStatement ps = this.connection.prepareStatement(s);
            ps.setString(1, author_name);
            r=ps.executeQuery();
            while(r.next()){
                String s1=r.getString("book_name");
                String s2=r.getString("category");
                String s3=r.getString("fpy");
                QueryResult.ResultQ3 tmp = new QueryResult.ResultQ3(s1,s2,s3);
                re.add(tmp);
            }
            res=re.toArray(new QueryResult.ResultQ3[0]);
            ps.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return res;
    }

    @Override
    public QueryResult.ResultQ4[] functionQ4() {
        ResultSet rs;
        QueryResult.ResultQ4[] res=null;
        String s = "SELECT DISTINCT publisher_id, category from book b where b.publisher_id = (SELECT p.publisher_id from publisher p where p.publisher_name like (\"% % %\")) AND 3 < (Select AVG(b.rating) from book b where b.publisher_id IN (SELECT p.publisher_id from publisher p where p.publisher_name like (\"% % %\"))) AND 2 < (Select COUNT(*) from book b where b.publisher_id IN (SELECT p.publisher_id from publisher p where p.publisher_name like (\"% % %\"))) ORDER BY publisher_id, category asc;";
        ArrayList<QueryResult.ResultQ4> re=new ArrayList<QueryResult.ResultQ4>();
        try{
            PreparedStatement ps = this.connection.prepareStatement(s);
            rs=ps.executeQuery();
            while(rs.next()){
                int s1=rs.getInt("publisher_id");
                String s2=rs.getString("category");
                QueryResult.ResultQ4 tmp = new QueryResult.ResultQ4(s1,s2);
                re.add(tmp);
            }
            res=re.toArray(new QueryResult.ResultQ4[0]);
            ps.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
        
        
        return res;
    }

    @Override
    public QueryResult.ResultQ5[] functionQ5(int author_id) { //TODO
        ResultSet rs;
        QueryResult.ResultQ5[] res=null;
        String s = "Select author_name, author_id from author where author_id in (SELECT author_id from book b, author_of ao where b.isbn = ao.isbn AND ao.author_id not in (select author_id from book bo, author_of aof where aof.isbn=bo.isbn AND bo.publisher_id not in (Select publisher_id from book boo, author_of a where boo.isbn=a.isbn AND a.author_id = ?)));";
        ArrayList<QueryResult.ResultQ5> re=new ArrayList<QueryResult.ResultQ5>();
        try{
            PreparedStatement ps = this.connection.prepareStatement(s);
            ps.setInt(1, author_id);
            rs=ps.executeQuery();
            while(rs.next()){
                int s1=rs.getInt("author_id");
                String s2=rs.getString("author_name");
                QueryResult.ResultQ5 tmp = new QueryResult.ResultQ5(s1,s2);
                re.add(tmp);
            }
            res=re.toArray(new QueryResult.ResultQ5[0]);
            ps.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
        
        
        return res;
        
        
        
    }

    @Override
    public QueryResult.ResultQ6[] functionQ6() {
        QueryResult.ResultQ6[] res=null;
        String s ="SELECT author_id, isbn FROM author_of ao WHERE ao.author_id IN (SELECT ao.author_id FROM author_of ao,book b,(SELECT publisher_id FROM book where publisher_id NOT IN (SELECT DISTINCT t1.publisher_id from (SELECT b.publisher_id, ao.author_id from book b, author_of ao WHERE b.isbn=ao.isbn) as t1,(SELECT b.publisher_id, ao.author_id from book b, author_of ao WHERE b.isbn=ao.isbn) as t2 WHERE t1.publisher_id = t2.publisher_id AND t1.author_id <> t2.author_id))as an WHERE b.publisher_id = an.publisher_id AND ao.isbn=b.isbn);";
        ResultSet rs;
        ArrayList<QueryResult.ResultQ6> re=new ArrayList<QueryResult.ResultQ6>();
        try{
            PreparedStatement ps = this.connection.prepareStatement(s);
            rs=ps.executeQuery();
            while(rs.next()){
                int s1=rs.getInt("author_id");
                String s2=rs.getString("isbn");
                QueryResult.ResultQ6 tmp = new QueryResult.ResultQ6(s1,s2);
                re.add(tmp);
            }
            res=re.toArray(new QueryResult.ResultQ6[0]);
            ps.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        return res;
    }

    @Override
    public QueryResult.ResultQ7[] functionQ7(double rating) {
        String s ="Select p.publisher_id ,p.publisher_name from (SELECT publisher_id FROM book where category=\"Roman\" group by publisher_id having avg(rating) > ? AND Count(publisher_id) > 1) as a,publisher p where a.publisher_id=p.publisher_id ORDER BY p.publisher_id asc;";
        QueryResult.ResultQ7[] res=null;
        ArrayList<QueryResult.ResultQ7> re=new ArrayList<QueryResult.ResultQ7>();
        ResultSet r;
        try{
            PreparedStatement ps = this.connection.prepareStatement(s);
            
            ps.setDouble(1, rating);
            r=ps.executeQuery();
            while(r.next()){
                int s1=r.getInt("publisher_id");
                String s2=r.getString("publisher_name");
                QueryResult.ResultQ7 tmp = new QueryResult.ResultQ7(s1,s2);
                re.add(tmp);
            }
            res=re.toArray(new QueryResult.ResultQ7[0]);
            ps.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return res;



    }

    @Override
    public QueryResult.ResultQ8[] functionQ8() {
        QueryResult.ResultQ8[] res=null;
        ArrayList<QueryResult.ResultQ8> re=new ArrayList<QueryResult.ResultQ8>();
        ResultSet r;
        String s ="INSERT INTO phw1\n" +
                "SELECT b1.isbn as isbn,b1.book_name as book_name,b1.rating as rating FROM book b1,book b2 where b1.isbn <> b2.isbn AND b1.book_name = b2.book_name AND b1.rating < b2.rating;";
        try{
            PreparedStatement ps = this.connection.prepareStatement(s);
            
            
            ps.executeUpdate();
            ps.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
        s = "Select * from phw1;";
        try{
            PreparedStatement p = this.connection.prepareStatement(s);
            r=p.executeQuery();
            while(r.next()){
                String s1=r.getString("isbn");
                String s2=r.getString("book_name");
                double s3 = r.getDouble("rating");
                QueryResult.ResultQ8 tmp = new QueryResult.ResultQ8(s1,s2,s3);
                re.add(tmp);
            }
            res=re.toArray(new QueryResult.ResultQ8[0]);
            p.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return res;
    }

    @Override
    public double functionQ9(String keyword) {
        String s = "UPDATE book SET rating = rating + 1 Where rating < 4 AND book_name LIKE \"%is%\";";
        ResultSet r;
        double res=0.;
        try{
            PreparedStatement ps = this.connection.prepareStatement(s);
            ps.executeUpdate();
            ps.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
        try{
            s="select * from book";
            PreparedStatement p = this.connection.prepareStatement(s);
            r=p.executeQuery();
            while(r.next()){
                res += r.getDouble("rating");
            }
            p.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return res;
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int function10() {
        String s ="DELETE FROM publisher where publisher_id not in (Select publisher_id from book);";
        ResultSet r;
        int res=0;
        try{
            PreparedStatement ps = this.connection.prepareStatement(s);
            ps.executeUpdate();
            ps.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
        try{
            s="SELECT COUNT(*) as c from publisher;";
            PreparedStatement p = this.connection.prepareStatement(s);
            r=p.executeQuery();
            while(r.next()){
                res += r.getDouble("c");
            }
            p.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return res;
    }    
}
