#!/bin/bash
VERSION=1.0.0
TARGET=${PWD##*/}

DESCRIBE=`git describe --always`
REVERSION=`git rev-list --count --first-parent HEAD`
VERSION="$VERSION $REVERSION.$DESCRIBE"
BUILDDT=`echo $(date +%Y)-$(date +%m)-$(date +%d) $(date +%H):$(date +%M):$(date +%S)`

#go build -ldflags "-X 'main.VERSION=1.0.0 13.9bc27e0' -X 'main.BUILDDT=2022-04-19 17:11:15'" -o migrationOracle2Maria .


go build -ldflags "-X 'main.VERSION=${VERSION}' -X 'main.BUILDDT=${BUILDDT}'" -o migrationOracle2Maria .
./$TARGET -v