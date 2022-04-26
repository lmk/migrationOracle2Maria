#!/bin/bash
VERSION=1.0.0
TARGET=${PWD##*/}

DESCRIBE=`git describe --always`
REVERSION=`git rev-list --count --first-parent HEAD`
VERSION="$VERSION $REVERSION.$DESCRIBE"
BUILDDT=`echo $(date +%Y)-$(date +%m)-$(date +%d) $(date +%H):$(date +%M):$(date +%S)`

go build -ldflags "-X 'main.VERSION=${VERSION}' -X 'main.BUILDDT=${BUILDDT}'" -o ${TARGET} .
./$TARGET -v
