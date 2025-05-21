#!/bin/bash
# Automate the source creation and tileset publication process.

if [[ $(brew list | grep 'gdal') ]]; then
  echo "GDAL is installed."
else
  brew install gdal
fi

if ! test -f venv/bin/activate; then
  echo "Could not locate python virtual environment. Attempting to create."
  if command -v python &> /dev/null; then
    python -m venv ./venv
  elif command -v python3 &> /dev/null; then
    python3 -m venv ./venv
  else
    echo "Could not create virtual environment."
    exit 1
  fi
  echo "Activating Python virtual environment."
  source venv/bin/activate
  python -m pip install 'mapbox-tilesets[estimate-area]'
else
  echo "Activating Python virtual environment."
  source venv/bin/activate
fi


file=""
username=""
tileset=""
token=""
minzoom=""
maxzoom=""

while getopts 'f:u:t:k:m:x:' flag; do
  case "${flag}" in
    f) file="${OPTARG}" ;;
    u) username="${OPTARG}" ;;
    t) tileset="${OPTARG}" ;;
    k) token="${OPTARG}" ;;
    m) minzoom="${OPTARG}" ;;
    x) maxzoom="${OPTARG}" ;;
  esac
done



if [[ -z "${token}" ]]; then
  echo "No Mapbox token found. Pass a value using -k."
  exit 1
fi

{
  echo "Uploading data to "$tileset"..."
  tilesets upload-source $username --replace --no-validation --token $token $tileset $file
} && {
  # # If source upload is successful, create tileset.
  existing_tilesets=$(tilesets list $username --token $token)
  if [[ $existing_tilesets =~ $username"."$tileset ]]; then
    echo $username"."$tileset" exists. Deleting..."
    tilesets delete $username"."$tileset --force --token $token
  fi
  echo '{
    "version": 1,
      "layers": {
        "geographies": {
          "source": "mapbox://tileset-source/'$username'/'$tileset'",
          "minzoom": '$minzoom',
          "maxzoom": '$maxzoom',
          "tiles": {
            "layer_size": 2500
          }
        }
    }
  }' > recipe.json
  echo "Creating tileset using recipe..."
  tilesets create $username"."$tileset --token $token --recipe recipe.json --attribution '[
    {
      "text": "MIT Spatial Analysis & Action Research Group",
      "link": "https://github.com/mit-spatial-action/who-owns-mass-processing"
    }
  ]' --name "$(echo $tileset | tr "_" " ")"
  rm recipe.json
} && {
  echo "Starting publishing process..."
  # # If tileset creation is successful, publish tileset.
  tilesets publish $username"."$tileset --token $token
}