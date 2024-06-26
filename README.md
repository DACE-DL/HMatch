# HMatch 

HMatch is a hybrid approach which combines embedding-based methods and link keys extraction methods both addressing the task of entity matching. Two directions have been tested:
- Direction 1: Extraction of link keys from a pair of sampled KGs. These KGs are sampled from the original ones using the identity links generated by embedding-based methods.
- Direction 2: Extracting sets of link keys which explain the identity links generated by embedding-based methods.

# KGs, identity links, and reference identity links accessibility

This repository contains the original DB15PK multi-lingual KGs. The original datasets are four: English (En.), French (Fr.), Chinese (Zh.) and Japanese (Ja.).

Memory-alpha and stexpanded KGs can be downloaded here: https://oaei.webdatacommons.org/tdrs/testdata/persistent/knowledgegraph/v4/knowledgegraph_v4.zip. 

The identity links obtained from BERT-INT and TransEdge and used in the paper for each of the KGs are respectively present in BERT-INT-IDL and TransEdge-IDL. 

The reference identity links used for the evaluation are present in References.


# Running
Direction 1:

- Step 1: Sampling of the KGs using sampling.py passing as arguments path/to/SourceKG path/to/TargetKG path/to/identitylinks path/to/sampledSourceKG  path/to/sampledTargetKG.
- Step 2: Launching Linkex on the sampled KGs, Linkex can be cloned and compiled from https://gitlab.inria.fr/moex/linkex.

Direction 2:

- Step 1: linkex is an alias, in bash you have to write first : alias  linkex='java -mx12G -jar  path/to/LINKEX//LinkkeyDiscovery-1.0-SNAPSHOT-jar-with-dependencies.jar'.
- Step 2:   linkex  -t in -f testselect -e path/to/identitylinks path/to/sampledSourceKG path/to/sampledTargetKG

# Evaluation
The script EvLinkex.py allows to evaluate the quality of the link keys extracted, passing the arguments path/to/Linkeys path/to/SourceKG path/to/TargetKG path/to/ReferenceLinks . It display the precision, recall and F-measure of the best 25 link keys and the average values of the top 10.
