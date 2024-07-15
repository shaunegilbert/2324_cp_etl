cc_template
==============================

aws sso login --profile name
sudo hwclock -s
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_rsa

#directions on how to build new docker file 
# Step into your project directory
cd /path/to/your/project

# Build a new Docker image with the custom Dockerfile
docker build -t s50_etl_image:latest -f Dockerfile.cp_etl .

# Stop the old container
docker stop cp_etl_container

# Remove the old container
docker rm cp_etl_container

# Remove the old image
docker rmi cp_etl_image:old_tag

# Run a new container with the new image
docker run -d --name cp_etl_container cp_etl_image:latest

# setting up new docker container to run updated code

# 1 build new image
docker build -f Dockerfile.cp_etl -t cp_etl_image:tag .

# 2 stop existing container
docker stop cp_etl_container

# 3 remove old docker container
docker rm cp_etl_container

# 4 run new container with new image
docker run --name cp_etl_container cp_etl_image:tag

# 5 cleanup old image
docker rmi old-image-name:old-tag

# 6 clean up old images
docker image prune -a


# This Docker command is used to run a container from an image, while also mounting AWS credentials into it.

# docker run: This command is used to create and start a container from a Docker image.

# -v ~/.aws:/root/.aws: 
# - '-v' flag is used for volume mounting. It mounts a volume from the host to the container.
# - '~/.aws' is the directory on the host machine that contains AWS credentials and configuration files.
# - '/root/.aws' is the target path inside the container where the host's AWS configuration will be mounted.
# This volume mounting ensures that the AWS credentials and configuration files are accessible inside the container, 
# allowing Boto3 or AWS CLI inside the container to use these credentials.

# --name cp_etl_container: 
# - '--name' flag assigns a name to the running container. In this case, the container will be named 'cp_etl_container'.
# Naming containers is useful for identifying and managing them, especially when dealing with multiple containers.

# cp_etl_image: 
# This is the name of the Docker image that the container will be created from. 
# 'cp_etl_image' should be an image that you have previously built, which contains your ETL application.

# Executing this command will start the 'cp_etl_container' container and mount the host's AWS credentials inside it,
# enabling the application within the container to access AWS services using these credentials.

# aws command probably won't be needed in prod

# run it detached (-d) then when you run start command it will work
docker run -d -v ~/.aws:/root/.aws --name cp_etl_container cp_etl_image

# start existing container
docker start cp_etl_container



------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.readthedocs.io


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
