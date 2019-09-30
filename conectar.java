/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package practica2;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.*;
import javax.swing.table.DefaultTableModel;

/**
 *
 * @author Brandon Banti
 */

public class conectar {
    Connection conect = null;
    DefaultTableModel uno;
        public conectar(String nombre)
		{
                    String url = "jdbc:postgresql://localhost:5432/"+nombre;
			try {
				//Cargamos el Driver MySQL
				//Class.forName("com.mysql.jdbc.Driver");
				//Class.forName("org.gjt.mm.mysql.Driver");
				//conect = DriverManager.getConnection("jdbc:mysql://172.16.20.238/zesati","root","");
				//System.out.println("conexion establecida ");
				//JOptionPane.showMessageDialog(null,"Conectado");                        
				//Cargamos el Driver Access
				//Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
				//Conectar en red base
				//String strConect = "jdbc:odbc:Driver=Microsoft Access Driver (*.mdb)";DBQ=//servidor/bd_cw/cw.mdb";
				//Conectar Localmente
				//String strConect = "jdbc:odbc:Driver=Microsoft Access Driver (*.mdb)";DBQ=:/cwnetbeans/cw.mdb";
				//conect = DriverManager.getConnection(strConect,"","");
                                Class.forName("org.postgresql.Driver");
                                conect = DriverManager.getConnection(url,"postgres","brandon0608");
                                System.out.println("Conexion establecida");
                             
			} catch (SQLException e) {
				System.out.println("error de conexion");
			} catch (ClassNotFoundException ex) {
            Logger.getLogger(conectar.class.getName()).log(Level.SEVERE, null, ex);
        }
		}
        public void desconectar(){
                    try{
                            conect.close();
                    }catch(Exception ex){}
                    
        }
        
        public double tipo(String nombre_tabla,int x)throws SQLException{
            Statement st=conect.createStatement();
            String resul1;
            double bytes=0.0;
            String resul2;
            String query="";
            ResultSet resultado;
            query="select *"
            +" from "+nombre_tabla+""
            +" limit 1";
        try{
            System.out.println(query);
            resultado = st.executeQuery(query);
            
            while (resultado.next()) {
                for(int i=0; i<x;i++){
                    resul1=resultado.getObject(i+1).getClass().toString();
                    String[] s1 = resul1.split(" ");
                    resul2=s1[1];
                    String[] s = resul2.split("[.]");
                    switch (s[2]){
                        case "Integer": 
                            bytes+=4;
                            break;
                        case "String":
                            bytes+=65325;
                            break;
                        case "Date":
                            bytes+=4;
                            break;
                        case "Boolean":
                            bytes+=1;
                            break;
                        default:
                            bytes+=4;
                            break;
                    }
                    
                    System.out.println(s[2] + " suma "+bytes);           
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(conectar.class.getName()).log(Level.SEVERE, null, ex);
        }
        return bytes;
    }
    public int num_tablas()throws SQLException{
            Statement st=conect.createStatement();
            String query="";
            int numEntero=0;
            String r1="";
            ResultSet resultado;
            query="SELECT count(table_name)"
            +" FROM information_schema.tables"
            +" WHERE table_schema='public'"
            +" AND table_type='BASE TABLE'";
        try{
            resultado = st.executeQuery(query);
            
            while (resultado.next()) {
                r1= resultado.getObject(1).toString();
                numEntero = Integer.parseInt(r1);
            }
        } catch (SQLException ex) {
            Logger.getLogger(conectar.class.getName()).log(Level.SEVERE, null, ex);
        }
        return numEntero;
    }
    public int num_columnas(String nombre)throws SQLException{
            Statement st=conect.createStatement();
            String query="";
            int numEntero=0;
            String r1="";
            ResultSet resultado;
            query="SELECT count(column_name)"
            +" FROM information_schema.columns"
            +" WHERE table_schema = 'public'"
            +" AND table_name   = '"+nombre+"'";
        try{
            resultado = st.executeQuery(query);        
            while (resultado.next()) {
                r1= resultado.getObject(1).toString();
                numEntero = Integer.parseInt(r1);
            }
        } catch (SQLException ex) {
            Logger.getLogger(conectar.class.getName()).log(Level.SEVERE, null, ex);
        }
        return numEntero;
    }
    public String [] tablas(int x)throws SQLException{
            Statement st=conect.createStatement();
            String query="";
            String [] resultadofinal = new String [x];
            ResultSet resultado;
            query="SELECT table_name"
            +" FROM information_schema.tables"
            +" WHERE table_schema='public'"
            +" AND table_type='BASE TABLE'";
        try{
            resultado = st.executeQuery(query);
            int i=0;
            while (resultado.next()) {
                resultadofinal[i]= resultado.getObject(1).toString();
                i++;
            }
        } catch (SQLException ex) {
            Logger.getLogger(conectar.class.getName()).log(Level.SEVERE, null, ex);
        }
        return resultadofinal;
    }
    
}
