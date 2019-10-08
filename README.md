# neossat2caom2
An application to generate CAOM2 Observations from NEOSSat FITS files.

# How To Run NEOSSat

In an empty directory (the 'working directory'), on a machine with Docker installed:

1. If running at CADC, ensure the group 'docker' has write and execute permissions on the empty directory:
    ```
    chmod 777 <empty dir>
    ```

1. On the machine with Docker installed, one time only, log in to the registry:

    ```
   > docker login bucket.canfar.net
   > Username: <user name>
   > Password: <password>
   ```

1. In the master branch of this repository, one time only, find the scripts directory, and copy the file neossat_run.sh to the working directory. e.g.:

   ```
   wget https://raw.github.com/opencadc-metadata-curation/neossat2caom2/master/scripts/neossat_run.sh
   ```

1. Ensure the script is executable, one time only:

   ```
   chmod +x neossat_run.sh
   ```

3. In the working directory, create a file named 'todo.txt'. Put the list of 
.fits files from the NEOSSat directories into this file.

4. To run the application:

    ```
    ./neossat_run.sh
    ```
