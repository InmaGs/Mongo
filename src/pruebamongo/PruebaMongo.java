/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package pruebamongo;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.Mongo;
import com.mongodb.DBCollection;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @author inmaculada.garcia
 */
public class PruebaMongo {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        
        System.out.println("Prueba de conexión con MongoDB");
               
        MongoClient mongo = conexionMongo();
        if(mongo!=null){
            System.out.println("Elementos de la colección contactos: ");
            //Cambiar este método según se quiera en cada caso.
            modificarElementos(mongo);
            obtenerDocumentosOrdenados(mongo);
        }
        else{
            System.out.print("Error: No se ha podido extablecer la conexión");
        }
    }
    
    private static MongoClient conexionMongo(){
        
    //Instanciamos objeto de tipo MongoClient
    MongoClient mc = null;
    
    //Bloque try-catch que establece la conexión si no hay errores.
    try{
        //Ruta en donde se aloja en servidor la base de datos
        mc = new MongoClient("localhost", 27017);
    }
    catch(Exception e){
        System.out.println("Error: "+e);
    }
    return mc;
    }
    
        private static void imprimirBasesDatos(MongoClient mongo){
            
            List basesdatos = mongo.getDatabaseNames();
            
            for(int i=0; i<basesdatos.size();i++){
                System.out.println("-"+basesdatos.get(i).toString());
            }
        }
        private static void obtenerColecciones(MongoClient mongo){
            List basesDatos = mongo.getDatabaseNames();
            for(int i = 0;i<basesDatos.size();i++){                
                //Obtenemos las bases de datos existentes:
                String nombredb = mongo.getDB(basesDatos.get(i).toString()).toString();
                System.out.println("-"+nombredb);
                //Usamos una base de datos en concreto:
                //if(!nombredb.equals("local")){
                DB db = mongo.getDB(nombredb);
                
                //Obtenemos el nombre de las colecciones que contiene la base de datos:
                Set colecciones = db.getCollectionNames();
                //Imprimimos la lista de colecciones si hay colecciones que mostrar:
                if(!colecciones.isEmpty())
                for(int j = 0; j<colecciones.size();j++){
                    Object col[] = colecciones.toArray();
                    System.out.println("   * "+col[j]);
                }
                else
                    System.out.println("   * Sin colecciones.");
            }
        }
        
        private static void obtenerDocumentos(MongoClient mongo){
            
            //Seleccionamos la base de datos 'prueba'
            DB db = mongo.getDB("prueba");
            //Seleccionamos la colección 'contactos'
            DBCollection col = db.getCollection("contactos");
            
            //Agregamos todos los elementos de la colección a un 
            //objeto de tipo cursor.
            DBCursor elementos = col.find();
            
            try{
                while(elementos.hasNext()){
                    //Seleccionamos cada elemento de la colección
                    //Y se imprime
                    DBObject obj = elementos.next();
                    System.out.println(obj);
                }
            }
            catch(Exception e){
                System.out.println("Error: "+e);
            }
            finally{
                elementos.close();
            }
        }
        
        private static void obtenerDocumentosOrdenados(MongoClient mongo){
            DB db = mongo.getDB("prueba");
            DBCollection col = db.getCollection("contactos");
            
            DBCursor elementos = col.find().sort(new BasicDBObject("_id",1));
            try{
                while(elementos.hasNext()){
                    //Seleccionamos cada elemento de la colección
                    //Y se imprime
                    DBObject obj = elementos.next();
                    System.out.println(obj);
                }
            }
            catch(Exception e){
                System.out.println("Error: "+e);
            }
            finally{
                elementos.close();
            }
        }
        
        private static void anadirDocumentos(MongoClient mongo){
            
            //Seleccionamos la base de datos 'prueba'
            DB db = mongo.getDB("prueba");
            //Seleccionamos la colección 'contactos' dentro de esa base de datos
            DBCollection col = db.getCollection("contactos");
            
            //Creamos objeto con los elementos que se han de añadir
            DBObject obj = new BasicDBObject().append("_id",12).
                    append("nombre", "Catelyn").append("apellido", "Stark");
            try{
                col.insert(obj);
                System.out.println("Elemento añadido satisfactoriamente.");
            }
            catch(Exception e){
                System.out.println("Error: "+e);
            }
        }
        
        private static void eliminarDocumentos(MongoClient mongo){
            DB db = mongo.getDB("prueba");
            DBCollection col = db.getCollection("contactos");
            try{
                //Instrucción que va a eliminar los elementos especificados
                col.remove(new BasicDBObject("nombre","Eddard"));
                System.out.println("Elemento elimado satisfactoriamente.");
            }
            catch(Exception e){
                System.out.println("Error: "+e);
            }
        }
        
        private static void modificarElementos(MongoClient mongo){
            DB db = mongo.getDB("prueba");
            DBCollection col = db.getCollection("contactos");
            try{
                //Instrucción que modifica los elementos
                col.update(new BasicDBObject("_id",8), //Id que queremos modificar
                        new BasicDBObject("$set",//operador para añadir un elemento
                        new BasicDBObject("muerto","Si")));//valor del nuevo elemento
                System.out.println("Elemento modificado satisfactoriamente.");
            }
            catch(Exception e){
                System.out.println("Error: "+e);
            }
        }
        
        
    
}
