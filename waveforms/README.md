#-----------------------------------------------------------------------------------------------------
Last update: 4/12/2021
By: Manlik Kwong Tufts Medical Center, Tufts CTSI

#-----------------------------------------------------------------------------------------------------

The waveform folder contains two project sub-folders pertaining to the handling of the MIMIC-IV waveform database. Please refer to the PhysioNet MIMIC-IV website for more information
about the MIMIC waveform demonstration and waveform database releases and documentation.

This sub-project is only focused on converting and mapping MIMIC-IV wave binary and CSV format data into an OMOP (ohdsi.org) common data model (CDM). Conversion of the MIMIC clinical data
is handled in a separate sub-project.

Subfolders:

Documents: This subfolder contains documentation describing the work done so far in converting various MIMIC-IV waveform data into an OMOP CDM. 

Tools: This subfolder contains software libraries (jar-files) and source code developed in an Eclipse Integrated Development Environment as well as example execution scripts for the 
various conversion and mapping tools used to transform native MIMIC-IV binary and CSV data into OMOP CDM load files.

All software developed in this project is written in the Java 1.8.x programming language and use various open source and proprietary libraries. The non-proprietary MIMIC-IV ETL source code 
funded by the N3C is provided here for your use. Be aware updates are not on a regular release schedule, so please check back for updates.

Where To Start:

1. Read the Documents/SignalMethods.pdf to get an overview of the ETL and conversion strategy and process.

2. Examples OMOP CSV load files are found in Documents/Examples/

3. If you are interested in experimenting with waveform data, first setup an Eclipse Integrated Development Environment and clone/copy the project including example run-time scripts. The 
   development environment was done on a Windows 10 environment, so the project may need some adjustments to work on Mac OS or other operating systems to compile and execute. 

