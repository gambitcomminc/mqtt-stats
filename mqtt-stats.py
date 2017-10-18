#!/usr/bin/env python

##############################################################################
## Copyright (c) 2017 by Gambit Communications, Inc.
## All Rights Reserved
## This is released under the GNU GPL 3.0
## https://www.gnu.org/licenses/gpl-3.0.en.html
##############################################################################

import os 
import getopt
import sys
import socket
import time
import logging
import threading
import multiprocessing
import webbrowser

# debug.setLogger(debug.Debug('all'))

formatting = '%(levelname)s %(asctime)s (%(module)s) - %(message)s'
logging.basicConfig(level=logging.DEBUG, format=formatting, )


import gi
gi.require_version('Gtk', '3.0')

try:
	from gi.repository import Gtk
	from gi.repository import GObject
except:
	logging.error ("require Gtk")
	sys.exit(1)

try:
	from Mimic.ErrorInfo import *
	from Mimic.SearchPath import *
except:
	logging.error ("require Mimic")
	sys.exit(1)

import paho.mqtt.client as mqtt

import json


###########################################################################


###########################################################################
debug = False
if debug:
  from colorlog import ColoredFormatter
  def setup_logger():
    """Return a logger with a default ColoredFormatter."""
    formatter = ColoredFormatter(
        "(%(threadName)-9s) %(log_color)s%(levelname)-8s%(reset)s %(message_log_color)s%(message)s",
        datefmt=None,
        reset=True,
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red',
        },
        secondary_log_colors={
            'message': {
                'ERROR': 'red',
                'CRITICAL': 'red',
                'DEBUG': 'yellow'
            }
        },
        style='%'
    )

    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    return logger

  # Create a player
  logger = setup_logger()

  from functools import wraps
  def trace(func):
    """Tracing wrapper to log when function enter/exit happens.
    :param func: Function to wrap
    :type func: callable
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.debug('Start {!r}'. format(func.__name__))
        result = func(*args, **kwargs)
        logger.debug('End {!r}'. format(func.__name__))
        return result
    return wrapper

else:

  from functools import wraps
  def trace(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        return result
    return wrapper

###########################################################################
class _IdleObject(GObject.GObject):
    """
    Override GObject.GObject to always emit signals in the main thread
    by emmitting on an idle handler
    """

    @trace
    def __init__(self):
        GObject.GObject.__init__(self)

    @trace
    def emit(self, *args):
        GObject.idle_add(GObject.GObject.emit, self, *args)


###########################################################################
class _UpdateThread(threading.Thread, _IdleObject):
	"""
	Cancellable thread which uses gobject signals to return information
	to the GUI.
	"""
	__gsignals__ = {
		"completed": (
		    GObject.SignalFlags.RUN_LAST, None, []),
		"progress": (
		    GObject.SignalFlags.RUN_LAST, None, [
		        GObject.TYPE_FLOAT])  # percent complete
	}

	@trace
	def __init__(self, parent):
		threading.Thread.__init__(self, target=self.update_main, name="Update Thread")
		_IdleObject.__init__(self)
		self.cancelled = False

	# main thread for the update thread
	# this thread periodically checks for any changes
	# and initiates updates
	@trace
	def update_main(self):
		while True:
			# wake up every second for response
			time.sleep (1)

			if main.is_stopped:
				break

			if main.is_paused:
				continue

			# but only do the work every N seconds
#			count += 1
#			if count < self.poller.poll_interval:
#				continue
#			count = 0

#			logging.debug ("update_cycle start")
			self.update_cycle()
#			logging.debug ("update_cycle completed")
			self.emit("completed")

		logging.debug ("done update_main")

	# run a poll cycle
	# if agents have changed, handle the first 100 immediately for
	# quick visual response, the rest later
	@trace
	def update_cycle(self):
#		logging.debug ("done update_cycle")
		return


###########################################################################
# Topic statistics
class Topic():
	def __init__(self, number, topic, bytes, last_time, last_payload, rowref):
		self.topic = topic
		self.number = number
		self.count = 1;		# seen at least once
		self.bytes = bytes
		self.last_time = last_time
		self.last_payload = last_payload
		self.rowref = rowref

	def display(self):
		print "Topic: %s, count %d, bytes %d" % (self.topic, self.count, self.bytes)

###########################################################################
# MQTT subscriber code
# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    logging.debug ("MQTT client connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(main.topic, main.qos)

# The callback for when a PUBLISH message is received from the server.
# updates metrics for later GUI display
def on_message(client, userdata, msg):
    if main.clear_stats:
	main.messages_received = 0
	main.topics = dict()
	main.num_topics = 0
	main.clear_stats = False
	main.clear_store = True

    bytes = len (msg.payload)
    timestamp = time.ctime (time.time())

#    logging.debug (msg.topic + ' ' + str(bytes) + ' ' + str(msg.payload))

    main.messages_received += 1

    # if not already there, add to set of topics detected
    if msg.topic not in main.topics:
    	main.num_topics += 1
	newtopic = Topic(main.num_topics, msg.topic, bytes, timestamp, msg.payload, None)
	main.topics[msg.topic] = newtopic
    else:
    	existingtopic = main.topics[msg.topic]
	existingtopic.count += 1
	existingtopic.bytes += bytes
	existingtopic.last_time = timestamp
	existingtopic.last_payload = msg.payload

    return


def on_disconnect(client, userdata, rc):

    if rc != 0:
    	logging.error ("unexpected disconnect: " + str(rc))

def subscriber_client(addr):
	client = mqtt.Client()
	client.on_connect = on_connect
	client.on_message = on_message
	client.on_disconnect = on_disconnect

	client.connect(main.host_ip, main.port_num, 60)

	client.loop_start()


###########################################################################
class MyApp:
	def __init__(self):
		self.host_ip = None
		self.port_num = None
		self.verbose = False
		self.topic = '#'
		self.qos = 0

		self.is_stopped = False
		self.is_paused = False

		self.messages_received = 0
		self.num_topics = 0
		self.topics = dict()
		self.clear_stats = False
		self.clear_store = False

	def usage(self):
		print ("Usage: mqtt-stats.py")
		print ("\t[-h|--host host]        broker to connect to; default localhost")
		print ("\t[-p|--port port]        port to connect to; default port 1883")
		print ("\t[-t|--topic topic]      topic; default #")
		print ("\t[-q|--qos qos]          QoS; default 0")
		print ("\t[-v|--verbose]    verbose output")
		return

	def start(self):
		self.command_line()

		self.show_gui()
		# from now on GUI is expected to be up

		subscriber_client (self.host_ip)

		self.update_thread = _UpdateThread(self)
		self.update_thread.connect("completed", self.completed_cb)
		self.update_thread.start()

		Gtk.main()

	###############################
	def command_line(self):
		try:
			opts, args = getopt.getopt(sys.argv[1:], "h:p:t:v", ["host=", "port=", "topic=", "qos=", "verbose"])
		except getopt.GetoptError as err:
			# print help information and exit:
			logging.error (str(err)) # will print something like "option -a not recognized"
			self.usage()
			sys.exit(1)

		for o, a in opts:
			if o in ("-v", "--verbose"):
			    self.verbose = True
			elif o in ("-h", "--host"):
				self.host_ip = a
			elif o in ("-p", "--port"):
				self.port_num = a
			elif o in ("-t", "--topic"):
				self.topic = a
			elif o in ("-q", "--qos"):
				self.qos = int(a)
			else:
			    assert False, "unhandled option"

		if self.host_ip == None:
			self.host_ip = "127.0.0.1"

		if self.port_num == None:
			self.port_num = 1883

	###############################
	def show_gui(self):
		self.builder = Gtk.Builder()
		dir_path = os.path.dirname(os.path.realpath(__file__))
		glade_path = dir_path+"/mqtt-stats.glade"
		self.builder.add_from_file(glade_path)
		self.builder.connect_signals(Handler())

		self.window = self.builder.get_object("mainWindow")

		# dialogs
		# Help->About
		self.aboutdialog = self.builder.get_object("aboutdialog1")

		# File->New
		self.filenewdialog = self.builder.get_object("filenewdialog")
		self.filenew_rb1 = self.builder.get_object("filenew_radiobutton1")
		self.filenew_rb2 = self.builder.get_object("filenew_radiobutton2")
		self.filenew_agents = self.builder.get_object("filenew_agents")

		self.errordialog = self.builder.get_object("errordialog")

		# status bar
		self.statusbar = self.builder.get_object("statusmessage")
		self.context_id = self.statusbar.get_context_id("status")
		self.clients = self.builder.get_object("clients")
		self.clients_context = self.statusbar.get_context_id("clients")
		self.freevm = self.builder.get_object("freevm")
		self.freevm_context = self.statusbar.get_context_id("freevm")
		self.activity_meter = self.builder.get_object("activitymeter")

		# the first titlelabel
		self.titlelabel = self.builder.get_object("titlelabel")
		self.titlelabel.set_text("MQTT Topic Statistics")

		self.infolabel1 = self.builder.get_object("infolabel1")
		self.infolabel1.set_text('MQTT Broker: ' + self.host_ip + '\nStarted: ' + time.ctime (time.time()))

		self.infolabel2 = self.builder.get_object("infolabel2")
		self.infolabel2.set_text("Topic: " + main.topic)

		self.infolabel3 = self.builder.get_object("infolabel3")
		self.infolabel3.set_text("")

		self.infolabel4 = self.builder.get_object("infolabel4")
		self.infolabel4.set_text("")

		# the liststore containing the agents
		self.topicstore = self.builder.get_object("topicstore")

		treeview = Gtk.TreeView(self.topicstore)
		self.treeview = treeview
		tvcolumn = Gtk.TreeViewColumn('Number')
		treeview.append_column(tvcolumn)
		cell = Gtk.CellRendererText()
		tvcolumn.pack_start(cell, True)
		tvcolumn.add_attribute(cell, 'text', 0)
		tvcolumn.set_sort_column_id(0)

		tvcolumn = Gtk.TreeViewColumn('Topic')
		treeview.append_column(tvcolumn)
		cell = Gtk.CellRendererText()
		tvcolumn.pack_start(cell, True)
		tvcolumn.add_attribute(cell, 'text', 1)
		tvcolumn.set_sort_column_id(1)

		tvcolumn = Gtk.TreeViewColumn('Count')
		treeview.append_column(tvcolumn)
		tvcolumn.pack_start(cell, True)
		tvcolumn.add_attribute(cell, 'text', 2)
		tvcolumn.set_sort_column_id(2)

		tvcolumn = Gtk.TreeViewColumn('Bytes')
		treeview.append_column(tvcolumn)
		tvcolumn.pack_start(cell, True)
		tvcolumn.add_attribute(cell, 'text', 3)
		tvcolumn.set_sort_column_id(3)

		tvcolumn = Gtk.TreeViewColumn('Time')
		treeview.append_column(tvcolumn)
		tvcolumn.pack_start(cell, True)
		tvcolumn.add_attribute(cell, 'text', 4)
		tvcolumn.set_sort_column_id(4)

		tvcolumn = Gtk.TreeViewColumn('Last Payload')
		treeview.append_column(tvcolumn)
		tvcolumn.pack_start(cell, True)
		tvcolumn.add_attribute(cell, 'text', 5)
		tvcolumn.set_sort_column_id(5)

		box = Gtk.VBox(False, 5)
		scrolledwindow = self.builder.get_object("scrolledwindow1")
		scrolledwindow.add(box)
		box.pack_start (treeview, True, True, 0)

		self.window.show_all()

	###############################
	# GUI component, could be separated out into a GUI class
	# the callback on every poller cycle
	@trace
	def completed_cb(self, thread):
#		logging.debug ("completed_cb")
		if main.clear_store:
			main.topicstore.clear()
			main.clear_store = False

		main.infolabel3.set_markup('<span foreground="blue">Messages received: ' + str(main.messages_received) + '</span>')
		main.infolabel4.set_markup('<span foreground="green">Topics: ' + str(len(main.topics)) + '</span>')

		# run through the topics and add to the matrix
		keys = main.topics.keys()
		for key in keys:
		    topic =  main.topics[key]
		    try:
			payloadstr = topic.last_payload.decode('utf-8')
			#    logging.debug ("payload is UTF-8 " + payloadstr)
		    except UnicodeError:
			payloadstr = topic.last_payload

		    if topic.rowref == None:

			# UTF-8 encodings
			try:
			    topicstr = topic.topic.decode('utf-8')
			#    logging.debug ("topic is UTF-8 " + topicstr)
			except UnicodeError:
			    topicstr = topic.topic

			rowref = main.topicstore.append(
				[
				topic.number,
				topicstr,
				1,
				topic.bytes,
				topic.last_time,
				payloadstr
				])

			topic.rowref = rowref
		    else:
		    	# redisplay only if new messages for topic
			displayedcount = main.topicstore.get_value (topic.rowref, 2)
			if topic.count != displayedcount:
				main.topicstore.set_value (topic.rowref, 2, topic.count)
				main.topicstore.set_value (topic.rowref, 3, topic.bytes)
				main.topicstore.set_value (topic.rowref, 4, topic.last_time)
				main.topicstore.set_value (topic.rowref, 5, payloadstr)


	def quit(self):
		# client.loop_stop()
		self.is_stopped = True
		return


###########################################################################
class Handler:
	def on_mainWindow_delete_event(self, *args):
		Gtk.main_quit(*args)
		main.quit()
	
	def on_gtk_quit_activate(self, menuitem, data=None):
		Gtk.main_quit()
		main.quit()

	# File->New menu handler
	def on_gtk_filenew_activate(self, menuitem, data=None):
		# clear everything
		main.infolabel3.set_text("")
		main.infolabel4.set_text("")

		main.clear_stats = True

	def on_filenew_radiobutton_toggled(self, rb, data=None):
		label = rb.get_label()
		enable_ok_button = True
		if rb == main.filenew_rb2:
			if rb.get_active():
				main.filenew_agents.set_editable(True)
				main.filenew_agents.set_visibility(True)
				if main.filenew_agents.get_text() == '':
					enable_ok_button = False
			else:
				main.filenew_agents.set_editable(False)
				main.filenew_agents.set_visibility(False)

		filenew_ok = main.builder.get_object("filenew_ok")
		if enable_ok_button:
			filenew_ok.set_sensitive(True)
		else:
			filenew_ok.set_sensitive(False)

	# Agent->Start menu handler
	def on_gtk_agentstart_activate(self, menuitem, data=None):
		treeselection = main.treeview.get_selection()
		(model, pathlist) = treeselection.get_selected_rows()
		for path in pathlist :
			tree_iter = model.get_iter(path)
			value = model.get_value(tree_iter,0)
			agent = main.session.get_agent(value)
			agent.start ()
		main.show_status_msg("started " + str(len(pathlist)) + " agents")


	def on_gtk_agentstop_activate(self, menuitem, data=None):
		treeselection = main.treeview.get_selection()
		(model, pathlist) = treeselection.get_selected_rows()
		for path in pathlist :
			tree_iter = model.get_iter(path)
			value = model.get_value(tree_iter,0)
			agent = main.session.get_agent(value)
			agent.stop ()
		main.show_status_msg("stopped " + str(len(pathlist)) + " agents")

	# Help->About menu handler
	def on_gtk_about_activate(self, menuitem, data=None):
		if main.session == None:
			license = "Not connected"
		else:
			license = main.session.get_licensing()
		main.aboutdialog.set_comments (license)
		self.response = main.aboutdialog.run()
		main.aboutdialog.hide()

###########################################################################
if __name__ == "__main__":
	GObject.threads_init()

	searchpath = SearchPath()

	main = MyApp()
	main.start()
