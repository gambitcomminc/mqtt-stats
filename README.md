# mqtt-stats
# MQTT Topic Statistics

## Overview

Brokers have extensive statistics in the $SYS topic, but not per-topic statistics.

This simple subscriber client displays per-topic statistics, eg. like mqtt-spy or mqtt-lens,
but more. It uses GTK to present a GUI. This utility allows you to analyze quantitatively
the published topics underneath a wildcard topic and answer such questions as "which topic
generates the most messages?" and "which topic generates the most traffic?". You can sort by
messages/second to get the most active topics. 

Initially, it displays epoch-wide statistics about the number and bytes for each sub-topic
of the specified topic. In the future we'll add time histograms of usage.

## Installation / Requirements

This python package requires

* Python 2.7.9 or 3.x
* PyGTK https://python-gtk-3-tutorial.readthedocs.io/en/latest/
* Eclipse Paho MQTT client API 1.5 https://www.eclipse.org/paho/clients/python/docs/

Tested on both Linux and Windows.

## Usage

Example usage:

./mqtt-stats.py --host mqtt.eclipse.org --topic '#' --qos 2

![screenshot](https://github.com/gambitcomminc/mqtt-stats/blob/master/mqtt-stats4.png)

If you use File->New it zeros out the collected topics, and will display the active topics from now on. This is because the broker publishes received "will" messages on all topics first. Most of those topics may no longer be active.


File -> Save dumps the topic statistics to the file dump.lst. 

The blog post at

https://gambitcomm.blogspot.com/2018/09/fire-and-forget-vs-complete-control-of.html

illustrates usage of this tool to investigate topic statistics.
