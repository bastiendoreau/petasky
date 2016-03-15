
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

/** Ce JAR permet de récuperer tous les fichiers (tableaux en csv) d'un repertoire défini en argument et de les enregistrer dans un JavaPairRDD <br/>
 *  La deuxième valeur de chaque ligne du fichier est filtrée et ajoutée à un résultat. <br/>
 *  Ce résultat est enregistré dans un fichier de destination défini en argument <br/>
 *  <br/>
 *  Ce JAR fonctionne en production avec Openstack en utilisant un répertoire de fichiers 'repcsv'. Il ne fonctionne pas avec le répertoire 'lsst' de 1.9 Tera en raison de problèmes de mémoire. <br/>
 *  
 * @author bastien doreau LIMOS
 *
 */
public class SparkManyFiles 
{

	private static JavaSparkContext context;

	public static void main(String[] args) 
	{
	    SparkConf conf = new SparkConf().setAppName("sparkmanyfiles");
	    context = new JavaSparkContext(conf);
	    
	    String inputFile = args[0];
	    String outputFile = args[1];
	    System.out.println("Utilisation wholeTextFiles");
	    
	    
	    ////////////////////////////////////////////////////
	    // Recupère tous les fichiers d'un répertoire donné
	    // Le résultat est un JavaPairRDD <String,String>    
	    // Le premier String est la clé soit le chemin du fichier
	    // Le deuxième String est la valeur soit le contenu du fichier
	    JavaPairRDD<String, String> fileContentsRDD = context.wholeTextFiles(inputFile);

    	/////////////////////////////////////////////////
    	// Cette fonction récupère la valeur du 2e champ 
	    // pour chaque ligne du fichier et les additionne
	       JavaRDD<String> sum2dField = fileContentsRDD.map(new Function<Tuple2<String, String>, String>() {
	    	  
			private static final long serialVersionUID = 1L;

				@Override
	            public String call(Tuple2<String, String> fileContent) throws Exception {
	                
					// recup du contenu du fichier
	            	String content = fileContent._2();

	            	boolean lastline=false;
	            	Long result=0L;
	            	
	            	while (lastline==false)
	            	{	
	            		int endline=content.indexOf("\n");
	            		
	            		// Si ce n'est pas la dernière ligne
	            		if ((endline!=-1) && (content.length()>3))
	            		{
		            		//recuperation d'une ligne à partir du contenu
		            		String line=content.substring(0, endline);
	
		            		// recuperation du contenu de la 2e valeur
		            		int virgule1=line.indexOf(",");
		            		line=line.substring(virgule1+1);
	
		            		int virgule2=line.indexOf(",");
		            		line=line.substring(0, virgule2);
		            			            		
		            		//Ajout du resultat 
		            		result = result + Long.parseLong(line);
		            		
		            		// suppression de la ligne en cours du contenu
		            		content=content.substring(endline+1);
	            		}
	            		
	            		// Si c'est la dernière ligne, sortie de la boucle while
	            		else
	            		{
	            		lastline=true;
	            		}
	            	}
	                
	               System.out.println(fileContent._1+" : Sum of values 2d field ->"+result);
	                return fileContent._1() + ": sum of values 2d field = " + result;
	            }
	        });
	    
	       
	       long total=sum2dField.count();
	       System.out.println("Nb total de fichiers = "+total);
	       
	       
	       //Enregistrement des résultats dans Swift ou un fichier texte 
	       sum2dField.saveAsTextFile(outputFile);
	}
}
