# neossat2caom2
An application to generate CAOM2 Observations from NEOSSat FITS files.

# How To Run NEOSSat

In an empty directory (the 'working directory'), on a machine with Docker installed:

1. In the master branch of this repository, one time only, find the scripts directory, and copy the file state.yml to the working directory. e.g.:

   ```
   wget https://raw.github.com/opencadc/neossat2caom2/master/scripts/state.yml
   ```

1. In the master branch of this repository, one time only, find the scripts directory, and copy the file neossat_run_state.sh to the working directory. e.g.:

   ```
   wget https://raw.github.com/opencadc/neossat2caom2/master/scripts/neossat_run_state.sh
   ```

1. Ensure the script is executable, one time only:

   ```
   chmod +x neossat_run_state.sh
   ```

1. To run the application:

    ```
    ./neossat_run_state.sh
    ```


# How to Run NEOSSat for CAOM2.4 Fixes

In an empty directory (the 'working directory'), on a machine with Docker installed:

1. Get the execution script and the configuration file:


   ```
   wget https://raw.github.com/opencadc-metadata-curation/neossat2caom2/master/scripts/config.yml
   wget https://raw.github.com/opencadc-metadata-curation/neossat2caom2/master/scripts/neossat_run.sh   
   ```
  
1. Near the end of the file, modify the config.yml file to have:

   ```
   task_types:
     - ingest_obs
   features:
     supports_composite: False
     use_file_names: False
     use_urls: False
   ```

1. Create a text file file named `todo.txt`. This file will have all the observation IDs that require repair, one ID per line. e.g.:

   ```
   2019141003230
   2019141230810
   ...
   ```


1. Ensure the script is executable:

   ```
   chmod +x neossat_run.sh
   ```

1. To run the application:

   ```
   ./neossat_run.sh
   ```
