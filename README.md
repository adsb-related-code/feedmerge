# FeedMerge

FeedMerge is a many-to-many network aggregator for streaming protocols where messages both fit in a single send() and are not dependent on ordering.

## Usage
**Installation**

    go get github.com/mikedamm/feedmerge

**Running**

    $ $GOPATH/bin/feedmerge \
	    --msgsize [size] \
	    --inport [port] \
	    --outport [port] \

**Options**

 - `msgsize` Maximum size of an individual message. Pick a number slightly larger than the maximum message size used by the protocol you are transporting. This amount of memory is allocated per inbound client.
 - `inport`  Clients connecting to this port are expected to feed a stream of messages.
 - `outport` Clients connecting to this port will receive a combined stream of all messages.

![enter image description here](https://scrutinizer-ci.com/g/mikedamm/feedmerge/badges/quality-score.png?b=master)  ![enter image description here](https://scrutinizer-ci.com/g/mikedamm/feedmerge/badges/build.png?b=master)
