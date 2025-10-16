# neossat2caom2
An application to generate CAOM2 Observations from NEOSSat FITS files.

# How To Run NEOSSat

In an empty directory (the 'working directory'), on a machine with Docker installed:

1. One time only:
   
   1. In the master branch of this repository, find the scripts directory, and copy the file `state.yml` to the working directory. e.g.:

      ```
      wget https://raw.github.com/opencadc/neossat2caom2/master/scripts/state.yml
      ```
   
   1. In the master branch of this repository, find the config directory, and copy the file `config.yml` to the working directory. e.g.:

      ```
      wget https://raw.github.com/opencadc/neossat2caom2/master/config/config.yml
      ```


   1. In the master branch of this repository, find the scripts directory, and copy the file neossat_run_state.sh to the working directory. e.g.:

      ```
      wget https://raw.github.com/opencadc/neossat2caom2/master/scripts/neossat_run_state.sh
      ```

   1. Ensure the script is executable:

      ```
      chmod +x neossat_run_state.sh
      ```

   1. Set up the file transfer location. `neossat2caom2` can be configured to work one of two ways: 1) move files post-transfer 2) leave files in place post-transfer. 
      1. For Option (1), there are three directories required: (a) the pick-up directory, (b) the successful transfer directory, and (c) the failed transfer directory. 
         1. Create the pick-up directory. If the pick-up directory will have a directory hierarchy, the `success` and `failure` locations must be rooted in a separate directory hierarchy. Add the following fragment to the `docker run` command in `neossat_run_state.sh`: `--mount "type=bind,src=<fully-qualified pick-up directory name here>,dst=/data"`
         2. Create the successful transfer directory (default name `success`). This can be a sub-directory of the pick-up directory, or a different path. If it's a different path, add the following fragment to the `docker run` command in `neossat_run_state.sh`: `--mount "type=bind,src=<fully-qualified successful transfer directory name here>,dst=/data/success"`
         3. Create the failed transfer directory (default name `failure`). This can be a sub-directory of the pick-up directory, or a different path. If it's a different path, add the following fragment to the `docker run` command in `neossat_run_state.sh`: `--mount "type=bind,src=<fully-qualified failed transfer directory name here>,dst=/data/failure"`
         4. Edit the `config.yml` file accordingly:
            1. Set `use_local_files: True`
            1. Set 
            
               ```
               data_sources: 
                 - data
               ```
               
            1. Set 
            
               ```
               data_source_extensions: 
                 - .fits
               ```
               
            1. If the pick-up directory will have a hierarchy, set `recurse_data_sources: True`. If the `success` and `failure` directories are sub-directories of the pick-up directory, this cannot be `True`.
            1. Set `cleanup_files_when_storing: True`
            1. Set `cleanup_success_destination: /data/success`
            1. Set `cleanup_failure_destination: /data/failure`

      2. For Option (2), there is one directory required: (a) the pick-up directory.
         1. Create the pick-up directory 
         1. Add the following fragment to the `docker run` command in `neossat_run_state.sh`: `--mount "type=bind,src=<fully-qualified pick-up directory name here>,dst=/data"`
         1. Edit the `config.yml` file accordingly: 
            1. Set `use_local_files: True`
            1. Set 
            
               ```
               data_sources: 
                 - data
               ```
               
            1. Set 
            
               ```
               data_source_extensions: 
                 - .fits
               ```
               
            1. If the pick-up directory will have a hierarchy, set `recurse_data_sources: True`
            1. Set `cleanup_files_when_storing: False`

1. The application requires a X509 certificate for authentication and authorization. The following commands will generate a valid 10-day proxy. Note that you will be prompted for the `Password:`

   ```
   docker run --rm -ti --mount "type=bind,src=${PWD},dst=/usr/src/app" opencadc/neossat2caom2 /bin/bash
   cadcops@c34e8321f722:/usr/src/app$ cadc-get-cert --days-valid 10 --cert-file /usr/src/app/cadcproxy.pem -u <CADC User Name>
   Password:
   exit
   ```

1. To run the application:

    ```
    ./neossat_run_state.sh
    ```

# Release Notes

Creating a new GitHub Release will trigger a GitHub Action to build and publish a new Docker image to Docker Hub. The image will be tagged with the GitHub Release version number.
