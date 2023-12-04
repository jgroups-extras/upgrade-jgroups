#!/bin/bash

mvn clean release:prepare -DignoreSnapshots=true -Darguments="-DskipTests=true -Dmaven.skip.javadoc=true"

