import rdflib
from rdflib import Namespace, URIRef,Graph
import re
import sys
from urllib.parse import quote
from collections import defaultdict
import unicodedata
import xml.etree.ElementTree as ET
import string
from collections import Counter
import subprocess
# Function to encode URIs

def calculate_quality_KG(sparql_queries, graph1, graph2, references):
    actual_matches = extract_sameas_pairs(references)
    total_subjects_count = len(set(graph1.subjects())) + len(set(graph2.subjects()))
    first_elements1 = [t[0] for t in sparql_queries]
    first_elements2 = [t[1] for t in sparql_queries]
    s_lk = []
    for query1, query2 in zip(first_elements1, first_elements2):
        #print(query1, query2)
        set_ab = set()
        first_iteration = True
        for q1, q2 in zip(query1, query2):
            result1_dict = {}
            result2_dict = {}
            # Execute the queries
            result1 = graph1.query(q1)
            result2 = graph2.query(q2)
            # Process results from both graphs
            for result, result_dict in [(result1, result1_dict), (result2, result2_dict)]:
                for row in result:
                    subject = row[0]
                    #print(subject)
                    object_value = normalize_string(str(row[-1]))
                    result_dict.setdefault(object_value, set()).add(str(subject))
                common_keys = set(result1_dict.keys()) & set(result2_dict.keys())
                intersection = set()
                for key in common_keys:
                    intersection.update((a, b) for a in result1_dict[key] for b in result2_dict[key])
            if first_iteration:
                set_ab.update(intersection)
                first_iteration = False
            else:
                set_ab.intersection_update(intersection)

            # Debug: print set_ab after update

        if set_ab:
            precision, recall, hmean = calculate_precision_recall(set_ab, actual_matches)
            s_lk.append((precision, recall, hmean))
            print("Precision, Recall, F-measure:", precision, recall, hmean)
        #else:
            #print("Can't calculate precision or recall because there are no individuals satisfying the lk")
    return s_lk
def is_malformed(uri):
    # Check for the specific pattern that indicates a malformed URI
    return uri.startswith("http:///")

def correct_uri(uri):
    # Correct the malformed URI by removing the extra slash
    return uri.replace("http:///", "http://")

def fix_malformed_uris(input_file, output_file):
    graph = Graph()
    graph.parse(input_file, format="xml")

    corrected_graph = Graph()

    for s, p, o in graph:
        if isinstance(s, URIRef) and is_malformed(str(s)):
            s = URIRef(correct_uri(str(s)))
        if isinstance(o, URIRef) and is_malformed(str(o)):
            o = URIRef(correct_uri(str(o)))

        corrected_graph.add((s, p, o))

    corrected_graph.serialize(destination=output_file, format="xml")

def encode_uri(uri):
    return quote(uri, safe=':/')
def compile_java():
    try:
        result = subprocess.run(['./compile_and_run_java.sh'], capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Compilation Error: {e}")
        print(f"Output: {e.output}")
        return False
    return True

def execute_sparql_query(dataset_file, sparql_query):
    jena_lib_dir = "./apache-jena-fuseki-5.0.0"
    classpath = f"{jena_lib_dir}/*:."
    command = [
        "java", "-cp", classpath, "SPARQLQueryExecutor", dataset_file,  sparql_query
    ]

    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return result
    except subprocess.CalledProcessError as e:
        print(f"Execution Error: {e}")
        print(f"Output: {e.output}")
        return None

def run_sparql_query_on_datasets(dataset_file, sparql_query):
    #print(f"Running SPARQL query on dataset: {dataset_file, sparql_query}")
    if compile_java():
        #print("Java compilation successful")
        result = execute_sparql_query(dataset_file, sparql_query)
        if result is None:
            print("Execution failed. Please check the Java program and the provided datasets.")
        return result
    else:
        print("Compilation failed. Please check the compile_and_run_java.sh script.")
        return None
        #return None

def normalize_string(s):
    if s is None:
           return None
    # Convert to lowercase
    # Convert non-string inputs to string
    if not isinstance(s, str):
        s = str(s)

    s = s.lower()

    # Remove punctuation
    s = s.translate(str.maketrans('', '', string.punctuation))

    # Remove accents
    s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

    # Remove whitespace
    s = s.strip()

    return s
def link_key_to_sparql_query(link_key, namespaces):
    set_query1=[]
    set_query2=[]

    for predicate1,predicate2 in link_key:
            filter_conditions11 = ' '.join({f'?subject1 {predicate1} ?o1 .'})
            filter_conditions21 = ' '.join({f'?subject2 {predicate2} ?o2 .'})
            object_variables1 = re.findall(r'\?o1', filter_conditions11)
            object_variables2 = re.findall(r'\?o2', filter_conditions21)

        # Constructing the SPARQL query
            query1 = f"""{namespaces}
            SELECT ?subject1 {' '.join(str(item) for item in object_variables1)} WHERE {{ {filter_conditions11}}}"""
            query2 = f"""{namespaces}
            SELECT ?subject2 {' '.join(str(item) for item in object_variables2)} WHERE {{ {filter_conditions21}}}"""
            set_query1.append(query1)
            set_query2.append(query2)

    return set_query1, set_query2

def generate_sparql_queries_from_file(file_path,namespaces):
    sparql_queries = []
    i = 0
    first_line = True
    with open(file_path, 'r') as file:
        for line in file:
            i+=1
            # Splitting the line into its components
            parts = line.strip().split('\t')
            if i<=25:
                link_key_str = parts[-4].strip()[1:-1]  # Extracting the link key string
                #(http://dbpedia.org/property/years,ns1:founded)
                #(http://dbpedia.org/property/areaUrbanKm,ns1:市街地面積(平方キロ)_),
                link_key_pairs=[]
                pairs = [pair for pair in link_key_str[1:-1].split('),(') if pair]

                for pair in pairs:
                    if ')_' not in pair:
                        # Process the pair normally
                        processed_pair = pair.strip().replace('),', ',').split(',')
                    else:
                        # Handle pairs that contain `)_`
                        processed_pair = pair.strip().split(',')
                        #processed_pair=processed_pair[:-1]

                    # Ensure the pair has two elements before adding to the list
                    if len(processed_pair) == 2:
                        link_key_pairs.append(processed_pair)
               #print(link_key_pairs)
                link_key = []
                for pair in link_key_pairs:
#pair.append(')')
                    if len(pair) != 2:
                        #print(f"Skipping malformed pair: {pair}")
                        i-=1
                        continue
                    predicate, object = pair
                    if '/' in predicate.strip():
                        predicate = "<" + encode_uri(predicate.strip()) + ">"
                    if '/' in object.strip():
                        object = "<" + object.strip() + ">"
                    if "\'" in object:
                        object = URIRef(object.strip().replace("'", ""))
                    if "\'" in predicate:
                        predicate = URIRef(predicate.strip().replace("'", ""))
                    link_key.append((predicate.strip(), object.strip()))
                sparql_query1, sparql_query2 = link_key_to_sparql_query(link_key,namespaces)
                sparql_queries.append((sparql_query1, sparql_query2))
            else:
                break

        return sparql_queries



def calculate_precision_recall(predicted_set, actual_set):
    #print("predicted set: ",predicted_set)
    #print("actual set: ",predicted_set)
    true_positives = 0
    false_positives = 0
    false_negatives = 0

    # Calculate true positives, false positives, and false negatives
    for element in predicted_set:
        if element in actual_set:
            true_positives += 1
        else:
            false_positives += 1

    for element in actual_set:
        if element not in predicted_set:
            false_negatives += 1

    # Calculate precision and recall
    precision = true_positives / (true_positives + false_positives) if (true_positives + false_positives) > 0 else 0
    recall = true_positives / (true_positives + false_negatives) if (true_positives + false_negatives) > 0 else 0
    hmean = (2 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    return precision, recall, hmean


# Example usage:
# Define sets of predicted and actual matches



def filter_duplicates(graph1, graph2):
    # Create a set to store unique object values from graph2
    unique_objects_graph2 = {o for _, _, o in graph2}

    # Create new graphs to store matching triples
    matching_graph1 = Graph()
    matching_graph2 = Graph()

    # Iterate over each triple in the first graph
    for s1, p1, o1 in graph1:
        # Check if the object value of the triple in graph1 is in unique_objects_graph2
        if o1 in unique_objects_graph2:
            # If a match is found, keep the triple in both graphs
            matching_graph1.add((s1, p1, o1))
            # Add triples from graph2 that match the object value
            for s2, p2, o2 in graph2:
                if o1 == o2:
                    matching_graph2.add((s2, p2, o2))
                    break  # Break the inner loop since we found a match

    return matching_graph1, matching_graph2

def filter_lines(input_file1, input_file2):
    matching_lines_file1 = set()
    matching_lines_file2 = set()

    # Read all lines from file2 into memory
    with open(input_file2, 'r') as file2:
        lines_file2 = set(file2)
    # Process each line from file1
    with open(input_file1, 'r') as file1:
        for line1 in file1:
            # Extract the last part from the line
            parts1 = line1.strip().split()
            obj1 = ' '.join(parts1[2:])
            if '@en' in obj1:
                obj1 = obj1.split('@')[0]
            # Compare each line of file1 to all lines of file2
            for line2 in lines_file2:
                parts2 = line2.strip().split()
                obj2 = ' '.join(parts2[2:])
                if '@fr' in obj2 or '@ja' in obj2 or '@zh' in obj2:
                    obj2 = obj2.split('@')[0]
                if obj1 == obj2:
                    matching_lines_file1.add(line1)
                    matching_lines_file2.add(line2)

    # Write matching lines to output files
    with open('graph1.ttl', 'w') as destination_file1:
        destination_file1.writelines(matching_lines_file1)

    with open('graph2.ttl', 'w') as destination_file2:
        destination_file2.writelines(matching_lines_file2)
    # Parse the files into RDF graphs
    graph1 = Graph()
    graph2 = Graph()

    graph1.parse('graph1.ttl', format="ttl")
    graph2.parse('graph2.ttl', format="ttl")
    #graph1, graph2=filter_duplicates(graph1, graph2)

    return graph1, graph2


def sort_lines_by_fourth_tab(file_path):
    # Read lines from the file into a list
    with open(file_path, 'r') as file:
        lines = file.readlines()
    # Separate the header from the rest of the lines
    header = lines[0]
    data_lines = lines[1:]
    # Define a custom sorting function to extract the fourth tab's value
    def get_fourth_tab_value(line):
        tabs = line.strip().split('\t')
        # Check if there are at least 4 tabs and the fourth value is a digit
        if len(tabs) > 3:
            try:
                value = float(tabs[3])
                if value > 0.1:
                    return value
            except ValueError:
                return -float('inf')  # Use negative infinity for non-numeric values
        return -float('inf')  # Default for lines that don't meet the criteria
    # Sort the data lines based on the value of their fourth tab in descending order
    sorted_lines = sorted(data_lines, key=get_fourth_tab_value, reverse=True)
    # Write the header and sorted lines back to the file
    with open(file_path, 'w') as file:
        file.write(header)
        file.writelines(sorted_lines)

def extract_sameas_pairs(file_path):
    sameas_pairs = set()

    with open(file_path, 'r') as file:
        for line in file:
            parts = line.strip().split('\t')
            if len(parts) == 3 and parts[1] == '<http://www.w3.org/2002/07/owl#sameAs>':
                #print(parts[0][1:-1], parts[2][1:-1])
                sameas_pairs.add((parts[0][1:-1], parts[2][1:-1]))
    return sameas_pairs



def calculate_quality_average(sparql_queries, graph1, graph2, references):
    actual_matches=extract_sameas_pairs(references)
    first_elements1 = [t[0] for t in sparql_queries]
    first_elements2 = [t[1] for t in sparql_queries]
    iteration_count = 0
    s_lk=[]
    for query1, query2 in zip(first_elements1, first_elements2):
            iteration_count+=1
            if iteration_count >= 11:  # Check if we have already done 10 iterations
                break
            set_ab = set()
            first_iteration = True
            for q1, q2 in zip(query1, query2):
                result1_dict = {}
                result2_dict = {}
                result1 = run_sparql_query_on_datasets(sys.argv[2], q1)
                result2 = run_sparql_query_on_datasets(sys.argv[3], q2)
                #print(result1,result2)
                if result1.returncode == 0 & result2.returncode == 0:
                    output_lines1 = result1.stdout.encode('utf-8').splitlines()
                    output_lines2 = result2.stdout.encode('utf-8').splitlines()
                for result, result_dict in [(output_lines1, result1_dict), (output_lines2, result2_dict)]:
                    for row in result:
                        #print(row)
                        if isinstance(row, bytes):
                            row = row.decode('utf-8')
                            columns = row.split(',')
                            if len(columns) < 2:
                                continue
                            subject = columns[0]
                            object_value = normalize_string(str(columns[1]))
                            result_dict.setdefault(object_value, set()).add(subject)
                    common_keys = set(result1_dict.keys()) & set(result2_dict.keys())
                    intersection = set()
                    for key in common_keys:
                        intersection.update((a, b) for a in result1_dict[key] for b in result2_dict[key])
                    if first_iteration:
                        set_ab.update(intersection)
                        first_iteration = False
                    else:
                        set_ab.intersection_update(intersection)
            if len(set_ab) > 0:
                precision, recall, hmean = calculate_precision_recall(set_ab, actual_matches)
                s_lk.append((precision, recall, hmean))
                print("precision, recall, fmeas:", precision, recall, hmean)
    return s_lk


def calculate_quality(sparql_queries, graph1, graph2, references):
    actual_matches=extract_sameas_pairs(references)
    first_elements1 = [t[0] for t in sparql_queries]
    first_elements2 = [t[1] for t in sparql_queries]
    s_lk=[]
    #print(len(first_elements1))
    #print(len(first_elements2))
    for query1, query2 in zip(first_elements1, first_elements2):
            set_ab = set()
            first_iteration = True
            for q1, q2 in zip(query1, query2):
                result1_dict = {}
                result2_dict = {}
                result1 = run_sparql_query_on_datasets(graph1, q1)
                result2 = run_sparql_query_on_datasets(graph2, q2)
                #print(result1,result2)
                if result1 is not None and result2 is not None:
                    if result1.returncode == 0 & result2.returncode == 0:
                        output_lines1 = result1.stdout.encode('utf-8').splitlines()
                        output_lines2 = result2.stdout.encode('utf-8').splitlines()
                    for result, result_dict in [(output_lines1, result1_dict), (output_lines2, result2_dict)]:
                        for row in result:
                            #print(row)
                            if isinstance(row, bytes):
                                row = row.decode('utf-8')
                                columns = row.split(',')
                                if len(columns) < 2:
                                    continue
                                subject = columns[0]
                                object_value = normalize_string(str(columns[1]))
                                result_dict.setdefault(object_value, set()).add(subject)
                        common_keys = set(result1_dict.keys()) & set(result2_dict.keys())

                        intersection = set()
                        for key in common_keys:
                            intersection.update((a, b) for a in result1_dict[key] for b in result2_dict[key])

                    if first_iteration:
                        set_ab.update(intersection)
                        first_iteration = False
                    else:
                        set_ab.intersection_update(intersection)
            if len(set_ab) > 0:
                precision, recall, hmean = calculate_precision_recall(set_ab, actual_matches)
                s_lk.append((precision, recall, hmean))
                print("precision, recall, fmeas:", precision, recall, hmean)
            #else:
                #print('here')
    return s_lk

def remove_language_tags(input_file_path, output_file_path, language_tags):
    """
    Remove specified language tags from the object part of triples in the input file
    and write the modified triples to the output file.
    Args:
    - input_file_path (str): Path to the input file containing RDF triples.
    - output_file_path (str): Path to the output file where modified triples will be written.
    - language_tags (list of str): List of language tags to remove from the object part of triples.
    """
    # Open the input file and output file
    with open(input_file_path, "r") as input_file, open(output_file_path, "w") as output_file:
        # Iterate through each line in the input file
        for line in input_file:
            # Split the line by the first occurrence of whitespace to extract subject, predicate, and object parts
            parts = line.strip().split(None, 2)
            if len(parts) == 3:
                subject, predicate, obj = parts
            else:
                # If there are not enough parts, skip this line
                continue

            # Remove the specified language tags from the object part if present
            for tag in language_tags:
                obj = obj.replace(tag, '')

            # Write the modified triple to the output file
            output_file.write(f"{subject} {predicate} {obj}\n")
def average_first_10_triplets(triplet_set):
    # Convert the set to a list
    triplet_list = list(triplet_set)

    # Take the first 10 items or less if the set has less than 10 items
    first_10_triplets = triplet_list[:10]

    # If there are no triplets in the set, return None
    if not first_10_triplets:
        print("The set is empty.")
        return None

    # Initialize sums for each position in the triplets
    sum1, sum2, sum3 = 0, 0, 0

    # Sum each element of the triplets
    for triplet in first_10_triplets:
        sum1 += triplet[0]
        sum2 += triplet[1]
        sum3 += triplet[2]

    # Calculate averages
    avg1 = sum1 / len(first_10_triplets)
    avg2 = sum2 / len(first_10_triplets)
    avg3 = sum3 / len(first_10_triplets)

    # Return the averages as a triplet
    return (avg1, avg2, avg3)

def extract_sameAs_relations(input_file_path, output_file_path):
    tree = ET.parse(input_file_path)
    root = tree.getroot()

    # Iterate through each <map> element in the Alignment
    with open(output_file_path, 'w') as f:
        for child in root:
        # Check if the child element is a <map> element
            if child.tag.endswith('map'):
            # Extract data from the <Cell> element under the <map>
                entity1 = child.find('.//{http://knowledgeweb.semanticweb.org/heterogeneity/alignment}entity1').attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource']
                entity2 = child.find('.//{http://knowledgeweb.semanticweb.org/heterogeneity/alignment}entity2').attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource']

            # Write the extracted information to the output file
                f.write(f"<{entity1}>\t<http://www.w3.org/2002/07/owl#sameAs>\t<{entity2}>\n")

# Example usage:
file_path = sys.argv[1]
 # Replace 'link_keys_bert_enfr.txt' with the path to your file containing link keys
sort_lines_by_fourth_tab(file_path)
# Define namespaces
foaf = Namespace("http://xmlns.com/foaf/0.1/")
skos=Namespace("http://www.w3.org/2004/02/skos/core#")
rdfs=Namespace("http://www.w3.org/2000/01/rdf-schema#")
ns1_fr = Namespace("http://fr.dbpedia.org/property/")
ns2_fr = Namespace("http://fr.dbpedia.org/property/jusqu'")
ns1_ja = Namespace("http://ja.dbpedia.org/property/")
ns1_zh = Namespace("http://zh.dbpedia.org/property/")


# Create a string to store namespaces
namespace_string = ""

# Add namespaces based on conditions
if 'fr' in sys.argv[3]:
    namespace_string += f"PREFIX foaf: <{foaf}> "
    namespace_string += f"PREFIX ns1: <{ns1_fr}> "
    namespace_string += f"PREFIX ns2: <{ns2_fr}> "
    sparql_queries = generate_sparql_queries_from_file(file_path, namespace_string)
    s_lk=calculate_quality(sparql_queries, sys.argv[2], sys.argv[3], sys.argv[4])
    #s_lk=calculate_quality_average(sparql_queries, graph1, graph2, sys.argv[4])
    sorted_slk = sorted(s_lk, key=lambda x: x[2], reverse=True)
    print(average_first_10_triplets(sorted_slk))

elif 'ja' in sys.argv[3]:
    namespace_string += f"PREFIX foaf: <{foaf}> "
    namespace_string += f"PREFIX ns1: <{ns1_ja}> "
    sparql_queries = generate_sparql_queries_from_file(file_path, namespace_string)
    s_lk=calculate_quality(sparql_queries, sys.argv[2], sys.argv[3], sys.argv[4])
    #s_lk=calculate_quality_average(sparql_queries, graph1, graph2, sys.argv[4])
    sorted_slk = sorted(s_lk, key=lambda x: x[2], reverse=True)
    print(average_first_10_triplets(sorted_slk))

elif 'zh' in sys.argv[3]:
    namespace_string += f"PREFIX foaf: <{foaf}> "
    namespace_string += f"PREFIX ns1: <{ns1_zh}> "
    sparql_queries = generate_sparql_queries_from_file(file_path, namespace_string)
    s_lk=calculate_quality(sparql_queries, sys.argv[2], sys.argv[3], sys.argv[4])
    #s_lk=calculate_quality_average(sparql_queries, graph1, graph2, sys.argv[4])
    sorted_slk = sorted(s_lk, key=lambda x: x[2], reverse=True)
    print(average_first_10_triplets(sorted_slk))

else:
    namespace_string += f"PREFIX foaf: <{foaf}> "
    namespace_string += f"PREFIX skos: <{skos}> "
    namespace_string += f"PREFIX rdfs: <{rdfs}> "

    ns1 = Namespace("http://dbkwik.webdatacommons.org/ontology/")
    namespace_string += f"PREFIX ns1: <{ns1}> "
    ns2 = Namespace("http://dbkwik.webdatacommons.org/memory-alpha.wikia.com/property/")
    namespace_string += f"PREFIX ns2: <{ns2}> "
    ns2 = Namespace("http://dbkwik.webdatacommons.org/ontology/")
    #ns3 = Namespace("http://www.w3.org/2004/02/skos/core#")
    #namespace_string += f"PREFIX ns3: <{ns3}> "
    graph1=Graph()
    graph2=Graph()
    sparql_queries = generate_sparql_queries_from_file(file_path, namespace_string)
    s_lk=calculate_quality_KG(sparql_queries, graph1.parse(sys.argv[2], format="xml"), graph2.parse(sys.argv[3], format="xml"), sys.argv[4])
    sorted_slk = sorted(s_lk, key=lambda x: x[2], reverse=True)
    print(average_first_10_triplets(sorted_slk))
