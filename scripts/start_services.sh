### This script is triggered from within docker contrainer
### to start multiple processes in the same container.
### This script is defined in the CMD option in Dockerfile

# Start actions server in background
rasa run actions --actions actions&

# Start rasa server with nlu model
rasa run --model /classbot/models --enable-api \
        --endpoints /classbot/config/endpoints.yml \
        --credentials /classbot/config/credentials.yml \
        -p $PORT