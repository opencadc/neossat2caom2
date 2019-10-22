# neossat2caom2
An application to generate CAOM2 Observations from NEOSSat FITS files.

# How To Run NEOSSat

In an empty directory (the 'working directory'), on a machine with Docker installed:

1. In the master branch of this repository, one time only, find the scripts directory, and copy the file state.yml to the working directory. e.g.:

   ```
   wget https://raw.github.com/opencadc-metadata-curation/neossat2caom2/master/scripts/state.yml
   ```

1. In the master branch of this repository, one time only, find the scripts directory, and copy the file neossat_run_state.sh to the working directory. e.g.:

   ```
   wget https://raw.github.com/opencadc-metadata-curation/neossat2caom2/master/scripts/neossat_run_state.sh
   ```

1. Ensure the script is executable, one time only:

   ```
   chmod +x neossat_run_state.sh
   ```

1. To run the application:

    ```
    ./neossat_run_state.sh
    ```
