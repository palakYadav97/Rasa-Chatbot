FROM rasa/rasa:2.7.0

USER root
ADD ./models /classbot/models/
ADD ./config /classbot/config/
ADD ./actions /classbot/actions/
ADD ./scripts /classbot/scripts/
ADD ./data /classbot/data/
ADD ./domain.yml /classbot/
ADD ./config.yml /classbot/

RUN chmod +x /classbot/scripts/*

ENTRYPOINT []
CMD /classbot/scripts/start_services.sh
