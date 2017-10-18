# mqtt-stats
MQTT Topic Statistics

Brokers have extensive statistics in the $SYS topic, but not per-topic statistics.

This simple subscriber client displays per-topic statistics, eg. like mqtt-spy or mqtt-lens,
but more. It uses GTK to present a GUI. This utility allows you to analyze quantitatively
the published topics underneath a wildcard topic and answer such questions as "which topic
generates the most messages?" and "which topic generates the most traffic?".

Initially, it displays epoch-wide statistics about the number and bytes for each sub-topic
of the specified topic. In the future we'll add time histograms of usage.

Example usage:

./mqtt-stats.py --host iot.eclipse.org --topic '#' --qos 2

