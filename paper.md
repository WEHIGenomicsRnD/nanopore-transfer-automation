---
title: 'Automating archiving and transfer of Nanopore sequencing data'
tags:
  - Snakemake
  - Nanopore
  - transfer
  - automation
authors:
  - name: Marek Cmero
    orcid: 0000-0001-7783-5530
    affiliation: "1, 2"
  - name: Inna Gupta
    affiliation: 1
affiliations:
 - name: Advanced Genomics Facility, Walter and Eliza Hall Institute of Medical Research
   index: 1
   ror: 01b6kha49
 - name: Department of Medical Biology, The University of Melbourne
   index: 2
   ror: 01ej9dk98
date: 05 September 2025
bibliography: paper.bib
---

# Summary

Nanopore sequencing is a class of fourth-generation sequencing technologies that involves passing nucleic acid strands through pores and measuring the electrical signal, though which the containing base-pairs are derived[citation]. Instruments, such as the Oxford Nanopore Technologies PromethION sequencer, generate large amounts data (often >1TB per flow cell), much of this attributed to the raw signal that is stored in fast5/pod5 filest. The Nanopore Transfer Automation pipeline is designed to streamline the process of moving this data in a robust and error-tolerant way from the sequencer machine to the destination via an automatable snakemake pipeline.

# Statement of need



# Citations

Citations to entries in paper.bib should be in
[rMarkdown](http://rmarkdown.rstudio.com/authoring_bibliographies_and_citations.html)
format.

If you want to cite a software repository URL (e.g. something on GitHub without a preferred
citation) then you can do it with the example BibTeX entry below for @fidgit.

For a quick reference, the following citation commands can be used:
- `@author:2001`  ->  "Author et al. (2001)"
- `[@author:2001]` -> "(Author et al., 2001)"
- `[@author1:2001; @author2:2001]` -> "(Author1 et al., 2001; Author2 et al., 2002)"

# Figures

- **Figure 1**: Workflow diagram showing processing steps in the Nanopore Transfer Automation tool.
![Workflow diagram.\label{fig:workflow}](workflow.png){ width=80% }

(Reference from text using \autoref{fig:example})

# Acknowledgements

We acknowledge WEHI ITS and Research Computing Platform, Quentin Gouil, Layla Wang and Rory Bowden for their support in this project.

# References