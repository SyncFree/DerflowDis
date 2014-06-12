#!/bin/bash

rm -r rel/derflowdis
make rel
rel/derflowdis/bin/derflowdis console
