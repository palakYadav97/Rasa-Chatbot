# Operational-Bot
## Open-Source-Chatbot using Rasa
This chatbot is developed using RASA Framework. This bot is created because the manual method of extracting the information might seem tedious and complicated. We now focus on what can be improved based on our previous method. It’s a time for us to learn from our past experiences with the objective of improving the way we work in future projects.

## Why Rasa?
Rasa is an open source framework to develop assistant or chatbot. The most important difference between other chatbot frameworks like Google DialogFlow, MS LUIS and Rasa is that, in Rasa all the development, training and testing in Rasa can be done locally and data remains private to the organization or the user. You can select your model to train the data, make alterations if you are not satisfied with the result.

## What is this Bot capable of?


## Set up
### Rasa Set up
```
mkdir rasa
cd rasa
```
create requirements.txt and paste this inside
```
ujson 
rasa==1.10.1
Pyhive==0.6.4
thrift==0.13.0
thrift_sasl==0.4.3
pandas
numpy
typed-ast
cpython
```
## About Rasa

There are many files generated after initialising Rasa. Few of them are listed below:
1. data/nlu.md

    This is the folder where you give examples of how the specific intents can be asked. For Intent (think of this as your intention to do something) like "greet", you would be asking "hi", "hello", "nice to meet you" and so on. This is where you list out these examples.
    ```
    ## intent:greet
    - hey
    - hello
    - hi
    ```
2. data/stories.md

This is where you are basically narrating. It is where you give examples of how the flow of the chat can go. This is where you write down as many possible path of the chatflow as you can. 
```
## happy path
* greet
  - utter_greet
* mood_great
  - utter_happy
```
3. domain.yml

You can basically think of this as the library. This is where you declare everything. All the intents that you are planning to use in your stories and nlu list them under intents. All the `actions` refer to what the bot will do when it detects a certain intent. Templates are for special kind of actions called utterances which is what the bot will reply. This is where utterances that were under `actions` get defined. 

There are also slots and entities that can be defined in this file. 
```
intents:
  - greet
  - goodbye


actions:
  - action_get_query
  - action_get_status
  - action_all_jobs
  - action_job_status


templates:
    utter_greet:
    - text: Hey there! This is DbBot. How can I help you?
    utter_iamabot:
    - text: 'I am a bot, powered by Rasa.'
    utter_helpmebot:
    - text: 'Alright! To see the full list, Click [here](https://github.optum.com/OBDP-CSG/ops-bot/blob/master/faq.md)'
    
```
4. endpoints.yml

    In this tutorial, we will only need to set one endpoint for custom action server.

5. actions.py

    This is where we can define all our custom actions to send to users when they ask something. A default template is generated.
6. config.yml

    Here is where we define the language, pipeline and the policies we are going to be using. In this tutorial, we are going to use `spacy_sklearn` instead of `supervised_embeddings`. So please replace as such:
    ```
    pipeline: spacy_sklearn
    ```
    This is where we can also define policies like fallback policy or twostage fallback policy for better intent handling. 
    
## Slack Integration  
 - train model: rasa train
 - train nlu: rasa train nlu
 - test interactive mode: rasa shell
 - run rasa using: rasa run
 - run actions server: rasa run actions
 - run rasa x: rasa x
