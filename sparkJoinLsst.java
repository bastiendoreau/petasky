package sparkjava;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


/** Cet éxécutable permet d'effectuer une jointure entre la table Object et la table ForcedSource dans la base lsst<br/>
 * Le champ "deepSourceId" et le champ "psfFlux" de la table ForcedSource sont récupérés ainsi que les champs "deepSourceId", "ra" et "decl" de la table Object.<br/>
 * Une jointure est réalisée sur le champ commun "deepSourceId".<br/>
 * Le résultat de la jointure étant du type JavaPairRDD<String, Tuple2<String, String>>, càd peu pratique à utiliser, toutes les valeurs sont concaténées dans un String.<br/>
 * Le résultat est donc un JavaRDD<String>
 * Ce résultat est enregistré dans le répertoire indiqué par le troisième argument<br/>
 * <br/>
 *  Au total trois arguments doivent être donnés : les deux tables en question ainsi qu'un répertoire où sera écrit le résultat.<br/> 
 * Exemple :
 * arg1: ForcedSource_100.32.csv    arg2: Object_10032.csv     arg3: sortieJoinlsst  <br/>
 * @author bdoreau LIMOS 29/04/2016
 *
 */
public class SparkLsst {
	
	private static JavaSparkContext context;
	
	public static void main(String[] args) {
		
		 SparkConf conf = new SparkConf().setAppName("sparklsst"); //.setMaster("local");

		    context = new JavaSparkContext(conf);
		    
		    // Marqueur temps début
		    long start = System.currentTimeMillis();
		    
		    ////////////////////////////////////////////////////////
		    // Recuperation des fichiers à ouvrir par les arguments : arg[0] et arg[1] 
		    // arg[0] correspond à ForcedSource et arg[1] correspond à Object
		    String inputFile_1 = args[0];
		    String inputFile_2 = args[1];
		    String outputFile = args[2];
		    
		    JavaRDD<String> file_1 = context.textFile(inputFile_1);
		    JavaRDD<String> file_2 = context.textFile(inputFile_2);
		    
		    //////////////////////////////////////////////////////////////////////////
		    // Temps pris par la récupération des fichiers avec la fonction textFile()
		    long after2TextFile = System.currentTimeMillis();
		    System.out.println("TextFile sur fichier 1 et fichier 2 "+(after2TextFile-start)+" ms");
		    
		    ///////////////////////////////////////////////////////////////////
		    // Décompte du nombre de lignes dans chacune des deux tables
		    // ainsi que le temps pris par ces deux méthodes count()
		    long nbValFile_1 = file_1.count();
		    System.out.println("Nb de valeurs fichier 1 :"+nbValFile_1);

		    long nbValFile_2 = file_2.count();
		    System.out.println("Nb de valeurs fichier 2 :"+nbValFile_2);
	
		    long after2Count = System.currentTimeMillis();
		    System.out.println("Count sur fichier 1 et fichier 2 "+(after2Count - after2TextFile)+" ms");
		    
		    ///////////////////////////////////////////////////////////////////////////////////
		    // Recupération dans ForcedSource du champ 1 : deepSourceId et du champ 3 : psfFlux
		    // Chaque ligne du csv est découpée après chaque ';', toutes les valeurs sont insérées dans un tableau
		    // la première valeur (deepSourceId) et la 3e valeur (psfFlux) sont récupérées de ce tableau 
		    // et écrites dans un Tuple qui sera inséré dans un JavaPairRDD<String, String> du type JavaPairRDD<"deepSourceID","psfflux">
		    JavaPairRDD<String, String> file1Pair = file_1.mapToPair(new PairFunction<String, String, String>(){
		    	public Tuple2<String, String> call(String s) {
	                String[] file1Tab = s.split(";");
	                return new Tuple2<String, String>(file1Tab[0], file1Tab[2]);
	            }
		    });
		    
		    ///////////////////////////////////////////////////////////////////////////////////////////
		    // Recupération dans Object du champ 1 : deepSourceId, du champ 2 : ra et du champ 3 : decl
		    // Les valeurs ra et decl sont concaténées dans un String séparées par un point-virgule
		    // On a donc encore un JavaPairRDD<String, String>  de type JavaPairRDD<"deepSourceID","ra;decl">  
		    JavaPairRDD<String, String> file2Pair = file_2.mapToPair(new PairFunction<String, String, String>(){
		    	public Tuple2<String, String> call(String s) {
	                String[] file2Tab = s.split(";");
	                return new Tuple2<String, String>(file2Tab[0], file2Tab[1]+";"+file2Tab[2]);
	            }
		    });
		    
		    ///////////////////////////////////////////////////////////////////////////////
		    // Calcul du temps pris par les 2 opérations de mapping (négligeable normalement) 
		    long after2MapToPair = System.currentTimeMillis();
		    System.out.println("MapToPair sur fichiers 1 et 2 :"+(after2MapToPair-after2Count)+" ms");
		    
		    
		    //////////////////////////////////////////////////////////////////////////////////
		    // Réalisation de la jointure entre les deux JavaPairRDD avec la fonction join()
		    // On obtient un JavaPairRDD<String, Tuple2<String, String>> de type : 
		    // JavaPairRDD<"deepSourceID", Tuple2<"psfFlux","ra,decl">>
		    JavaPairRDD<String, Tuple2<String, String>> join2files = file1Pair.join(file2Pair);
	        
		    long nbValJoin = join2files.count();
		    
		    long afterJoinCount = System.currentTimeMillis();
		    System.out.println("Jointure et count :"+(afterJoinCount- after2MapToPair)+" ms");
		    
		    System.out.println("Nb de valeurs apres jointure :"+nbValJoin);
		    
		    System.out.println("3 premiers elements de la jointure :"+join2files.take(3));
		    
		    
		    //////////////////////////////////////////////////////////////////////////////////
		    // Transformation de la jointure de type JavaPairRDD<"deepSourceID", Tuple2<"psfFlux","ra,decl">>
		    // en un JavaRDD<String> de type : JavaRDD<"deepSourceID,psfFlux,ra,decl"> 
		    JavaRDD<String> joinCleaned  = join2files.map(new Function <Tuple2<String, Tuple2<String, String>>,String>(){
				private static final long serialVersionUID = 1L;

				@Override
				public  String call(Tuple2<String, Tuple2<String, String>> joinContent) throws Exception {

					String result="";
					// recup du 1er String "deepSourceID" -> 1er champ du 1er tuple
					String contentDeepSourceId = joinContent._1();
					
					// recup du 2e tuple <"psfFlux","ra,decl"> -> 2e champ du 1er tuple
					Tuple2<String,String> contentTuple = joinContent._2();
					// recup du 2e String "psfFlux" -> 1er champ du 2e tuple
					String contentPsfflux = contentTuple._1();
					// recup du 3e String "ra,decl" -> 2e champ du 2e tuple
					String contentRaDecl  = contentTuple._2();
					
					// Concatenation de tous les champs dans un String
					result = contentDeepSourceId+";"+contentPsfflux+";"+contentRaDecl;
					return result;
				}
		    });
		    
		    long nbValJoinCleaned = joinCleaned.count();
		    System.out.println("Nb de valeurs apres clean jointure :"+nbValJoinCleaned);
		    long afterJoinCleanedCount = System.currentTimeMillis();
		    System.out.println("Temps clean Join + count :"+(afterJoinCleanedCount-afterJoinCount)+" ms");
		    
		    System.out.println("3 premiers elements de la jointure cleaned :"+joinCleaned.take(3));
		    
		    /////////////////////////////////////////////////////////////////////////////////
		    // Enregistrement du JavaRDD<String> créé dans le fichier passé dane le 3e argument
		    joinCleaned.saveAsTextFile(outputFile);
		    
		    
		    long  end = System.currentTimeMillis();
		    System.out.println("Temps d'enregistrement du JavaRDD nouvellement cree :"+(end-afterJoinCleanedCount)+" ms");
		    
		    System.out.println("Temps total : "+((end-start)/1000)+" s");
		    
		    
		    //////////////////////////////////////////
		    // Penser au context.stpo() pour éviter de 
		    // laisser un processus Spark ouvert et actif
		    context.stop();
	}

}
