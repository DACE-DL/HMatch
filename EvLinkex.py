import rdflib
from rdflib import Namespace, URIRef,Graph
import re
import sys
from collections import defaultdict
#
def link_key_to_sparql_query(link_key):
    set_query1=[]
    set_query2=[]

    for predicate1,predicate2 in link_key:
            filter_conditions11 = ' '.join({f'?subject1 {predicate1} ?o1 .'})
            filter_conditions21 = ' '.join({f'?subject2 {predicate2} ?o2 .'})
            object_variables1 = re.findall(r'\?o1', filter_conditions11)
            object_variables2 = re.findall(r'\?o2', filter_conditions21)

        # Constructing the SPARQL query
            query1 = f"""SELECT ?subject1 {' '.join(str(item) for item in object_variables1)} WHERE {{ {filter_conditions11}}}"""
            query2 = f"""SELECT ?subject2 {' '.join(str(item) for item in object_variables2)} WHERE {{ {filter_conditions21}}}"""
            set_query1.append(query1)
            set_query2.append(query2)

    return set_query1, set_query2

def generate_sparql_queries_from_file(file_path):
    sparql_queries = []
    i = 0
    first_line = True
    with open(file_path, 'r') as file:
        for line in file:
            if first_line:
                first_line = False
                continue  # Skip the first line
            i += 1
            # Splitting the line into its components
            parts = line.strip().split('\t')
            link_key_str = parts[-4].strip()[1:-1]  # Extracting the link key string
            link_key_pairs = [pair.strip()[1:].replace(')', '').split(',') for pair in link_key_str.split('),')]
            link_key = []
            for pair in link_key_pairs:
                if len(pair) != 2:
                    print(f"Skipping malformed pair: {pair}")
                    continue
                predicate, object = pair
                if '/' in predicate.strip():
                    predicate = "<" + predicate.strip() + ">"
                if '/' in object.strip():
                    object = "<" + object.strip() + ">"
                if "\'" in object:
                    object = URIRef(object.strip().replace("'", ""))
                if "\'" in predicate:
                    predicate = URIRef(predicate.strip().replace("'", ""))
                link_key.append((predicate.strip(), object.strip()))
            sparql_query1, sparql_query2 = link_key_to_sparql_query(link_key)
            sparql_queries.append((sparql_query1, sparql_query2))
            if i == 10:
                break
    return sparql_queries





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

    graph1.parse('graph1.ttl', format="turtle")
    graph2.parse('graph2.ttl', format="turtle")

    return graph1, graph2


def sort_lines_by_fourth_tab(file_path):
    # Read lines from the file into a list
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Define a custom sorting function to extract the fourth tab's value
    def get_fourth_tab_value(line):
        tabs = line.strip().split('\t')
        return tabs[3] if len(tabs) > 3 else ''

    # Sort the lines based on the value of their fourth tab
    sorted_lines = sorted(lines, key=get_fourth_tab_value, reverse=True)

    # Write the sorted lines back to the file
    with open(file_path, 'w') as file:
        file.writelines(sorted_lines)




# Example usage
with open(sys.argv[1], 'r') as file:
    lines = file.readlines()

filtered_lines = [line for line in lines if not line.split('\t')[-4] == '{}']

with open(sys.argv[1], 'w') as file:
    file.writelines(filtered_lines)

file_path = sys.argv[1]  # Replace 'link_keys_bert_enfr.txt' with the path to your file containing link keys
sort_lines_by_fourth_tab(file_path)
sparql_queries = generate_sparql_queries_from_file(file_path)
graph1 = Graph()
graph2 = Graph()

if 'ttl' in sys.argv[2]:
    graph1.parse(sys.argv[2], format="ttl")
if 'ttl' in sys.argv[3]:
    graph2.parse(sys.argv[3], format="ttl")
if 'xml' in sys.argv[2]:
    graph1.parse(sys.argv[2], format="xml")
if 'xml' in sys.argv[3]:
    graph2.parse(sys.argv[3], format="xml")

unique_subjects = set()

# Iterate over all triples in the graph
for subj, _, _ in graph1+graph2:
    # Add the subject to the set
    unique_subjects.add(subj)
# Get the total number of unique subjects
total_subjects_count = len(unique_subjects)
foaf = Namespace("http://xmlns.com/foaf/0.1/")

#ns1 = Namespace("http://dbpedia.org/property/")
if 'fr' in sys.argv[1]:
    ns1=Namespace("http://fr.dbpedia.org/property/")
    ns2=Namespace("http://fr.dbpedia.org/property/jusqu'")
    graph2.bind("ns2", ns2)
    #http://fr.dbpedia.org/property/
    graph1.bind("foaf", foaf)
    graph1.bind("ns1", ns1)
    graph2.bind("foaf", foaf)
    graph2.bind("ns1", ns1)
if 'ja' in sys.argv[1]:
    ns1=Namespace("http://ja.dbpedia.org/property/")
    graph1.bind("foaf", foaf)
    graph1.bind("ns1", ns1)
    graph2.bind("foaf", foaf)
    graph2.bind("ns1", ns1)
if 'zh' in sys.argv[1]:
    ns1=Namespace("http://zh.dbpedia.org/property/")
    graph1.bind("foaf", foaf)
    graph1.bind("ns1", ns1)
    graph2.bind("foaf", foaf)
    graph2.bind("ns1", ns1)
#skos=Namespace("http://www.w3.org/2004/02/skos/core#")
# Add namespace bindings to the graph


#graph2.bind("skos", skos)
# Get the namespaces from the graph
namespaces1 = dict(graph1.namespace_manager.namespaces())

# Bind the namespaces to your graph
for prefix, uri in namespaces1.items():
    graph1.bind(prefix, Namespace(uri))
namespaces2 = dict(graph2.namespace_manager.namespaces())

# Bind the namespaces to your graph
for prefix, uri in namespaces2.items():
    graph2.bind(prefix, Namespace(uri))
print("The number of individuals in both datasets: ", total_subjects_count)
j=0
average_dis = 0
average_cov = 0
count_dis=0
count_cov=0
first_elements1 = [t[0] for t in sparql_queries]
first_elements2 = [t[1] for t in sparql_queries]

for query1, query2 in zip(first_elements1, first_elements2):
    # Create dictionaries to store rows based on their last element

    print(query1)
    print(query2)
    set_ab = set()
    for q1, q2 in zip(query1, query2):
        result1_dict = {}
        result2_dict = {}
        #j += 1
        common_keys = set()
        result1 = graph1.query(q1)
        result2 = graph2.query(q2)
        set_a=set()
        set_b=set()
        # Store rows from result1 in result1_dict
        for row1 in result1:
            if isinstance(row1[-1], rdflib.term.Literal):
                last_element1 = row1[-1].value
            if isinstance(row1[-1], rdflib.term.URIRef):
                last_element1 = str(row1[-1])
            set_a.add((row1[0], last_element1))
        index_a = []
        index_b = []
        tuple_set_a=list(set_a)

        for element, corresponding_set in tuple_set_a:
            index = None
            for i, pair in enumerate(index_a):
                if pair == element:
                    index = i
                    break
            if index is not None:
                list(index_a)[index][1] += corresponding_set
            else:
                index_a.append((element, corresponding_set))
        for row2 in result2:
            if isinstance(row2[-1], rdflib.term.Literal):
                last_element2 = row2[-1].value
            if isinstance(row2[-1], rdflib.term.URIRef):
                last_element2 = str(row2[-1])
            set_b.add((row2[0], last_element2))
        tuple_set_b = list(set_b)
        for element, corresponding_set in tuple_set_b:
            index = None
            for i, pair in enumerate(index_b):
                if pair == element:
                    index = i
                    break
            if index is not None:
                list(index_b)[index][1].update(corresponding_set)
            else:
                index_b.append((element, corresponding_set))

        # in what follows, I create the index object-> subject for each element a,b in the source and the target data set

        result1_dict.update({corresponding_list: {element} for element, corresponding_list in index_a})

        result2_dict.update({corresponding_list: {element} for element, corresponding_list in index_b})

        #Now I create the set of common objects

        common_keys.update((set(result1_dict.keys()) & set(result2_dict.keys())))
        s_ab = set()
        # now since this key is common I can find it in the first and the second dictionary and I create the set of identity links
        for key in common_keys:
            s_ab.update(set((a, b) for a, b in zip(result1_dict[key], result2_dict[key])))

        if len(set_ab) > 0:
            set_ab &= s_ab
        else:
            set_ab = s_ab
    #after calculating the identity links, I need now to calculate the quality of this identity links
    first_elements = [pair[0] for pair in set_ab]
    second_elements = [pair[1] for pair in set_ab]
    inds = set(first_elements+second_elements)
    nbr_lks = len(set_ab)
    #who many times a is present as the first element in the pair
    counter_a = sum(1 for a in inds if a in first_elements)
    #who many times b is present as the second element in the pair
    counter_b = sum(1 for a in inds if a in second_elements)
    min_cardinality = min(counter_a, counter_b)
    if nbr_lks > 0:
        #favors the more discriminate side
        dis = min_cardinality / nbr_lks
        count_dis += dis
        coverage= len(inds)/total_subjects_count
        count_cov += coverage
        print("coverage: ", coverage)
        print("discriminability: ", dis)
    else:
        print("zero links generated, can't compute disc. or coverage")

av_dis = count_dis/10
av_cov = count_cov/10
print("Av cov: ", av_cov)
print("Av dis: ", av_dis)

if av_dis + av_cov > 0:
    hmean = 2 * av_dis * av_cov / (av_dis + av_cov)
    print("Av hmean: ", hmean)
