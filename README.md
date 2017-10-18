# mqtt-stats
MQTT Topic Statistics

Brokers have extensive statistics in the $SYS topic, but not per-topic statistics.

This simple subscriber client displays per-topic statistics, eg. like mqtt-spy or mqtt-lens,
but more. It uses GTK to present a GUI.

Initially, it displays epoch-wide statistics about the number and bytes for each sub-topic
of the specified topic.

Example usage:

./mqtt-stats.py --host iot.eclipse.org --topic '#' --qos 2

