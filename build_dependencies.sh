#!/usr/bin/env bash

# check to see if pipenv is installed
if [ -x "$(which pipenv)" ]
then
    # check that there Pipfile exists in root directory
    if [ ! -e Pipfile ]
    then
        echo 'ERROR - cannot find Pipfile'
        exit 1
    fi

    # use Pipenv to create a requirement.txt file
    echo '... creating requirements.txt from Pipfile'
    pipenv lock -r > requirements.txt

    # install packages to a temporary directory and zip it
    pip install -r requirements.txt --target ./packages

    # check to see if there are any external dependencies
    # if not then create an empty file to see zip with
    if [ -z "$(ls -A packages)" ]
    then
        touch packages/empty.txt
    fi

    # zip dependencies
    if [ ! -d packages ]
    then 
        echo 'ERROR - pip failed to import dependencies'
        exit 1
    fi

    cd packages
    zip -9mrv packages.zip .
    mv packages.zip ..
    cd ..

    # remove temporary directory and requirements.txt
    rm -rf packages
    rm requirements.txt
    
    # add local modules
    echo '... adding all modules from local utils package'
    zip -ru9 packages.zip dependencies -x dependencies/__pycache__/\*

    exit 0
else
    echo 'ERROR - pipenv is not installed --> run pip install pipenv to load pipenv into global site packages or install via a system package manager.'
    exit 1
fi