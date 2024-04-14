import sys
import rdflib


def retrieve_triples_with_subjects(dataset1_path, dataset2_path, alignment_file_path, output_file_path1,
                                   output_file_path2):
    # Create RDF Graphs for the datasets and alignments
    graph1 = rdflib.Graph()
    graph2 = rdflib.Graph()
    alignments = set()

    with open(dataset1_path, 'rb') as f_dataset1, open(dataset2_path, 'rb') as f_dataset2, open(alignment_file_path,
                                                                                                'r') as f_alignments:
        graph1.parse(f_dataset1, format='xml')
        graph2.parse(f_dataset2, format='xml')
        # Parse alignment file to collect pairs
        for line in f_alignments:
            parts = line.strip().split('\t')
            alignments.add((parts[0], parts[1]))

    # Create result graphs outside the loop
    result_graph1 = rdflib.Graph()
    result_graph2 = rdflib.Graph()

    # Find all triples for each graph to avoid repeated searches
    all_triples1 = list(graph1.triples((None, None, None)))
    all_triples2 = list(graph2.triples((None, None, None)))
    #print(len(all_triples1))

    # Open output files outside the loop
    with open(output_file_path1, 'wb') as f_output1, open(output_file_path2, 'wb') as f_output2:
       triples1_dict = {str(rdflib.term.URIRef(triple[0].replace("Special:FilePath/", "").lower())): triple for triple in all_triples1}
       triples2_dict = {str(rdflib.term.URIRef(triple[0].replace("Special:FilePath/", "").lower())): triple for triple in all_triples2}

       # Iterate over alignments and directly add triples to result_graph1 and result_graph2
       for alignment in alignments:
           subject1_uri, subject2_uri = alignment
           if subject1_uri in triples1_dict.keys():
               result_graph1.add(triples1_dict[subject1_uri])

           if subject2_uri in triples2_dict.keys():
               result_graph2.add(triples2_dict[subject2_uri])

       # Serialize and write the result graphs to separate output files
       with open(output_file_path1, 'wb') as f_output1, open(output_file_path2, 'wb') as f_output2:
           f_output1.write(result_graph1.serialize(format='turtle').encode('utf-8'))
           f_output2.write(result_graph2.serialize(format='turtle').encode('utf-8'))


def retrieve_triples_with_subjects_LIMES(dataset1_path, dataset2_path, alignment_file_path, output_file_path1,
                                   output_file_path2):
    # Create RDF Graphs for the datasets and alignments
    graph1 = rdflib.Graph()
    graph2 = rdflib.Graph()
    alignments = set()

    with open(dataset1_path, 'rb') as f_dataset1, open(dataset2_path, 'rb') as f_dataset2, open(alignment_file_path,
                                                                                                'r') as f_alignments:
        graph1.parse(f_dataset1, format='turtle')
        graph2.parse(f_dataset2, format='turtle')

        # Parse alignment file to collect pairs
        for line in f_alignments:
            parts = line.strip().split()
            if len(parts) == 4 and float(parts[2]) > 0.85:
                alignments.add((parts[0].strip('<').strip('>'), parts[2].strip('<').strip('>')))
                print(alignments.len)


    # Create result graphs outside the loop

    result_graph1 = rdflib.Graph()
    result_graph2 = rdflib.Graph()
    print(alignments.len)
    # Open output files outside the loop
    with open(output_file_path1, 'wb') as f_output1, open(output_file_path2, 'wb') as f_output2:
        for alignment in alignments:
            subject1_uri, subject2_uri = alignment
            #print(subject1_uri)
            #print(subject2_uri)
            # Find triples in the first dataset
            triples1 = graph1.triples((rdflib.URIRef(subject1_uri), None, None))
            for triple in triples1:
                result_graph1.add(triple)

            # Find triples in the second dataset
            triples2 = graph2.triples((rdflib.URIRef(subject2_uri), None, None))
            for triple in triples2:
                result_graph2.add(triple)

        # Serialize and write the result graphs to separate output files
        f_output1.write(result_graph1.serialize(format='turtle').encode('utf-8'))
        f_output2.write(result_graph2.serialize(format='turtle').encode('utf-8'))

#Save the result triples to the output files

#Example usage:
retrieve_triples_with_subjects(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[5], sys.argv[6])


