package sparkjava;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

/** Ce JAR récupère un fichier csv et opére plusieurs traitements.<br/>
* Récupération de certaines valeurs sur chaque ligne (tuple) et enregistrement en mémoire dans des JavaRDD<Long>. <br/>
* Filtrage de ces valeurs sur des critères précis ( > , > & < , == ) en enregistrement en mémoire ds de nouveaux JavaRDD.<br/>
* Décompte des résultats obtenus. <br/>
* Enregistrement d'un JavaRDD en base. <br/>
* <br/>
* Ce JAR est appliqué sur obj.csv (voir conteneur Swift sur Openstack), une base de type 'lsst' de 4 millions de tuples. <br/>
* Des marqueurs de temps permettent de quantifier le temps pris pour ces opérations. Comme attendu, les opérations de mapping sont négligeables (qq millisecondes) <br/>
* Les traitements sont effectués lors des 'count' et ces derniers sont de l'ordre de 30 sec sur Openstack. <br/>
* L'enregistrement final du JavaRDD en base dure environ 80 secondes.<br/>
* Résultats attendus - <br/>
* Valeur 1 Objectid  > 433327840000000 : 801171<br/>
* Valeur 3 ra_ps > 90° : 752629<br/>
* Valeur 55 ue1_sg > 0.17 & < 0.18 : 17360<br/>
* <br/>
*  
* @author bastien doreau LIMOS
*/
public class SparkGreaterThan2 {
	private static JavaSparkContext context;

	@SuppressWarnings("serial")
	public static void main(String[] args) {
		
		
	    SparkConf conf = new SparkConf().setAppName("sparkgreaterthan"); //.setMaster("local");

	    context = new JavaSparkContext(conf);
	    
	    //////////////////////////////////////////////////////////////////////////////////////
	    // Recuperation des arguments  arg 1 : fichier à ouvrir   ,  arg2 : fichier à sauver
	    String inputFile = args[0];
	    String outputDirectory = args[1];
	    
	    // Récupération de l'utilisateur
	    String user = context.sparkUser();
	    
	    
	    ///////////////////////////////////////////////////////////////////////////////////
	    // Récupération du fichier et enregistrement dans un JavaRDD
	    // Chaque ligne du fichier correspond à un String
	    //
	    //JavaRDD<String> file = context.textFile("hdfs://clusterspark-masterspark-001:8020/user/ubuntu/src/obj.csv");
	    JavaRDD<String> file = context.textFile(inputFile);
	    
	    
	    // Marqueur temps 1
	    long start = System.currentTimeMillis();

	    
/////////////////////////////////////////////////////////////////////////////
//////////  RECUPERATION DONNEES -> MAP ///////////////////////////
//////////////////////////////////////////////////////////////////////////
		
		//////////////////////////////////////////////////////////////
	    // Récupération de le 55e valeur de chaque ligne (Long) et 
	    // enregistrement dans un JavaRDD<Long>
		JavaRDD<Double> map1job2 = file.map(new Function<String, Double>() {
			  public Double call(String s) { 
				  String[] fields=s.split(",");
				  if (fields[54].equals(""))
				  {fields[54]="-1";}
				  Double goodVal=Double.parseDouble(fields[54]);
				  
				  return goodVal; 
				  }
			});
		map1job2.cache();
		// Le résultat JavaRDD<Long> map1job2 est mis en cache
		
		
		
		//////////////////////////////////////////////////////////////
	    // Récupération de le 1e valeur de chaque ligne (Long) et 
	    // enregistrement dans un JavaRDD<Long>
		JavaRDD<Long> map1job1=file.map(new Function<String, Long>(){
			@Override
			public Long call(String s) throws Exception {
				s=s.substring(0,s.indexOf(","));
				return Long.parseLong(s);
			}			
		});
		
	
		
		//////////////////////////////////////////////////////////////
	    // Récupération de le 3e valeur de chaque ligne (Long) et 
	    // enregistrement dans un JavaRDD<Long>
		
		JavaRDD<Double> map1job3 = file.map(new Function<String, Double>() {
			  public Double call(String s) { 
				  String[] fields=s.split(",");
				  if (fields[2].equals(""))
				  {fields[2]="-1";}
				  Double goodVal=Double.parseDouble(fields[2]);
				  
				  return goodVal; 
				  }
			});
		

		
		long endmaps=System.currentTimeMillis();
		long timeEndMaps= endmaps-start;
		
/////////////////////////////////////////////////////////////////////////////
////////// FIN RECUP ///////////////////////////
//////////////////////////////////////////////////////////////////////////

		
		
/////////////////////////////////////////////////////////////////////////////
////////// FILTRAGE DES DONNEE -> FILTER() /////////////////////////////////
//////////////////////////////////////////////////////////////////////////		
		

		
		///////////////////////////////////////////////////
		// Filtrage de la 1e valeur -> toutes les valeurs supérieures
		// à une valeur précise sont récupérées
		JavaRDD<Long> map2job1=map1job1.filter(new Function<Long,Boolean>(){
			@Override
			public Boolean call (Long i){
				if (i>433327840000000L) return true;
				else return false;
			}
		});
				
		
		///////////////////////////////////////////////////
		// Filtrage de la 3e valeur -> toutes les valeurs nulles
		// sont récupérées 
		JavaRDD<Double> map2job2=map1job3.filter(new Function<Double,Boolean>(){
			@Override
			public Boolean call (Double i){
				if (i>90) return true;
				else return false;
			}
		});
		
		///////////////////////////////////////////////////
		// Filtrage de la 55e valeur -> toutes les valeurs entre une 
		// valeur min et une valeur max sont récupérées
		JavaRDD<Double> map3job2=map1job2.filter(new Function<Double, Boolean>() {
			@Override
			public Boolean call(Double i) throws Exception {
				if (i>0.17 && i<0.18) return true;
				else return false;
			}
		});
		
		long endfilter=System.currentTimeMillis();
		
		long timeFilter= endfilter-endmaps;
		
/////////////////////////////////////////////////////////////////////////////
//////////FIN FILTRAGE ///////////////////////////
//////////////////////////////////////////////////////////////////////////		
		
		
		// Compte le resultat du filtre de la 1e valeur
		// fonction count()
		long nbVal1=map2job1.count();	
		
		long endcount1=System.currentTimeMillis();
		long timeCount1=endcount1-endfilter;
		
		// Compte le resultat du filtre de la 4e valeur
		// fonction count()
		long nbVal3=map2job2.count();
		
		long endcount3=System.currentTimeMillis();
		long timeCount3=endcount3-endcount1;
		
		
		// Compte le resultat du filtre de la 55e valeur
		// fonction count()
		long nbVal55=map3job2.count();
		
		long endcount55=System.currentTimeMillis();
		long timeCount55=endcount55-endcount3;
		
		///////////////////////////////////////////
		// Enregistrement du resultat de filtrage
		// de la 1e valeur dans le fichier donné 
		// en argument
		map2job1.saveAsTextFile(outputDirectory);

		long saveJavaRdd=System.currentTimeMillis();
		long timeSaveJavaRDD=saveJavaRdd-endcount55;
		
		// Calcule le temps total
		long timeTotal = System.currentTimeMillis() - start;
		
		

	   // Affichage
	   System.out.println("Nb lines Objectid > 433327840000000 :" +nbVal1);
	   System.out.println("Nb lines ra_ps > 90° :" +nbVal3);
	   System.out.println("Nb lines ue1_sg >0.17 & <0.18:" +nbVal55);
	   System.out.println("Spark user :" +user); 
	   System.out.println("\n"); 
	   System.out.println("timeTotal " +timeTotal/1000+" sec"); 
	   System.out.println("time Maps "+timeEndMaps+" millisec");
	   System.out.println("time Filters "+timeFilter+" millisec");
	   System.out.println("timeCount1 "+timeCount1/1000+" sec");
	   System.out.println("timeCount4 "+timeCount3/1000+" sec");
	   System.out.println("timeCount55 "+timeCount55/1000+" sec");
	   System.out.println("timeSaveFile "+timeSaveJavaRDD/1000+" sec");
	   

	   context.stop();
		
	}
}
