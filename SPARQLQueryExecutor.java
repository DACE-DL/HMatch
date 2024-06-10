import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.util.FileManager;

public class SPARQLQueryExecutor {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: SPARQLQueryExecutor <datasetFile> <query>");
            return;
        }

        String datasetFile = args[0];
        String sparqlQuery = args[1];

        // Load the local dataset
        Model model = ModelFactory.createDefaultModel();
        FileManager.get().readModel(model, datasetFile);

        // Execute the SPARQL query
        Query query = QueryFactory.create(sparqlQuery);
        try (QueryExecution qexec = QueryExecutionFactory.create(query, model)) {
            ResultSet results = qexec.execSelect();
            ResultSetFormatter.outputAsCSV(System.out, results);
        }
    }
}
