#-*- coding:utf-8 -*-

##############################################
# GARUDA CLIENT SDK
# Reference: Garuda Base Protocol Version 1.1
# Last Updated: 07-Aug-2015
##############################################

import sys
import time
import json
import socket
import threading

# Configuration for Garuda Core Connection
# These are for internal use of the SDK
# Note: The values are pre-configured and user MUST NOT change the values
_GAURDA_HOST = 'localhost'                   # Host address for Garuda Core
_GAURDA_PORT = 9000                          # Port number for Garuda Core
_GAURDA_ADDR = (_GAURDA_HOST, _GAURDA_PORT)  # Garuda Core Address
_BUFFER_SIZE = 2048                          # Socket Reader Buffer size 

# Protocol Request Messages
# These are for internal use of the SDK
_ID_ACTIVATE_GADGET_REQ = "ActivateGadgetRequest";
_ID_LOAD_GADGET_REQ = "LoadGadgetRequest";
_ID_GET_COMPATIBLE_GADGET_LIST_REQ = "GetCompatibleGadgetListRequest";
_ID_SEND_DATA_TO_GADGET_REQ = "SendDataToGadgetRequest";
_ID_LOAD_DATA_REQ = "LoadDataRequest";
_ID_SEND_NOTIFICATION_TO_GADGET_REQ = "SendNotificationToGadgetRequest";
_ID_SEND_NOTIFICATION_TO_CORE_REQ = "SendNotificationToCoreRequest";
_ID_TERMINATE_GADGET_REQ = "stop"

# Protocol Response Messages
# These are for internal use of the SDK
_ID_ACTIVATE_GADGET_RESP = "ActivateGadgetResponse";
_ID_LOAD_GADGET_RESP = "LoadGadgetResponse";
_ID_GET_COMPATIBLE_GADGET_LIST_RESP = "GetCompatibleGadgetListResponse";
_ID_SEND_DATA_TO_GADGET_RESP = "SendDataToGadgetResponse";
_ID_LOAD_DATA_RESP = "LoadDataResponse";
_ID_SEND_NOTIFICATION_TO_GADGET_RESP = "SendNotificationToGadgetResponse";

# Protocol Message Versions
# These are for internal use of the SDK
_REQ_MSG_VERSION = "0.2";
_RESP_MSG_VERSION = "0.2";

# Custom messages
# The SDK can generate during invocation of the callback method
# The callback handler in the gadget implementation should handle these
MSG_REMOTE_HOST_CLOSED = "RemoteHostClosedError"

# Custom IDs
# The SDK will use these as 'message_id' argument in the callback method invocation
# Each ID corresponds to an event/action within the SDK for which the callback method is invoked
# The callback handler in the gadget implementation MUST handle these IDs
ID_ACTIVATE_GADGET_RESPONSE = "activate_gadget_response";
ID_LOAD_GADGET_REQUEST = "load_gadget_request";
ID_GET_COMPATIBLE_GADGET_LIST_RESPONSE = "get_compatible_gadget_list_response";
ID_SEND_DATA_GADGET_RESPONSE = "send_data_to_gadget_response";
ID_LOAD_DATA_REQUEST = "load_data_request";
ID_LOAD_DATA_STREAM_REQUEST = "load_data_stream_request";
ID_SEND_NOTIFICATION_TO_GADGET_REQUEST = "send_notification_to_gadget_request";
ID_CONNECTION_TERMINATED = "connection_terminated";
ID_CONNECTION_NOT_INITIALIZED = "connection_not_initialized";
ID_JSON_PARSE_ERROR = "json_parse_error";
ID_JSON_DUMPS_ERROR = "json_dumps_error";

# Protocol Notification Types
# The SDK will use these as 'response_code' argument in the callback method invocation for 'message_id' ID_SEND_NOTIFICATION_TO_GADGET_REQUEST
# The callback handler in the gadget implementation should handle these on receiving ID_SEND_NOTIFICATION_TO_GADGET_REQUEST
NOTIFICATION_BRING_TO_FRONT = 602
NOTIFICATION_ERROR = 603
NOTIFICATION_TERMINATE = 604

# Response Codes
# The SDK will use these as 'response_code' argument in the callback method invocation for messages other than ID_SEND_NOTIFICATION_TO_GADGET_REQUEST
# The callback handler in the gadget implementation should handle these for the appropriate messages received
RESPCODE_SUCCESS = 200
RESPCODE_GADGET_ALREADY_CONNECTED = 400
RESPCODE_GADGET_APPKEY_MISMATCH = 401
RESPCODE_NO_COMPATIBLE_GADGET_FOUND = 403
RESPCODE_NO_CORE_DATABASE_CONNECTION = 404
RESPCODE_GADGET_NOT_FOUND_IN_CORE_DB = 405
RESPCODE_DATABASE_QUERY_ERROR = 409
RESPCODE_DATA_RESOURCE_NOT_FOUND = 415
RESPCODE_INTERNAL_ERROR = 500
RESPCODE_UNABLE_TO_PARSE_JSON = 501
RESPCODE_FILE_NOT_IN_OUTBOUND_LIST = 503
RESPCODE_INCOMPATIBLE_DATA_TYPE = 508
RESPCODE_GADGET_ALREADY_REGISTERED = 509
RESPCODE_INCOMPLETE_REQUEST_PARAMETERS = 512
RESPCODE_INVALID_NOTIFICATION_CODE = 513
RESPCODE_CORE_DB_OPERATION_FAILED = 515
RESPCODE_GADGET_NOT_ACTIVATED = 518
RESPCODE_BRING_TO_FRONT = 602
RESPCODE_ANY_ERROR_MESSAGES_FROM_CORE = 603
RESPCODE_TERMINATE_GADGETS = 604

####################################################################################################
# Classes for Custom Exception
# These are for internal use of the SDK
####################################################################################################

# Represents any Garuda related Exception
class GarudaException(Exception):
    pass

# Represents any improper connection state (for connection with Garuda Core)
class ImproperConnectionState(GarudaException):
    pass

# Represents any error in sending message/data to Garuda Core
class CannotSend(ImproperConnectionState):
    errno = None
    error_message = None
    def __init__(self, errno=None, error_message=None):
        self.errno = errno
        self.error_message = error_message

    def __str__(self):
        return 'connection terminate! errno: %s, error_message: %s' % (self.errno, self.error_message)

# Represents any error in receiving message/data from Garuda Core
class CannotRecv(ImproperConnectionState):
    pass

# Represents connection lost (for connection with Garuda Core)
class ConnectTerminated(ImproperConnectionState):
    pass

# Represents any error in creating connection with Garuda Core
class CannotConnect(ImproperConnectionState):
    pass

####################################################################################################
# Class representing a Gadget Entity
# The SDK will use this class to represent a gadget instance from the information received from Garuda Core
####################################################################################################
class Gadget:
    def __init__(self, gadget_name=None, gadget_id=None, gadget_iconpath=None, gadget_provider=None, gadget_gatewayid=None):
        self.gadget_name = gadget_name
        self.gadget_id = gadget_id
        self.gadget_iconpath = gadget_iconpath
        self.gadget_provider = gadget_provider
        self.gadget_gatewayid = gadget_gatewayid

    def __str__(self):
        result = 'gadget: name=%s\t\nid=%s\t\niconpath=%s\t\nprovider=%s\t\ngatewayid=%s'
        result = result % (self.gadget_name,
                           self.gadget_id,
                           self.gadget_iconpath,
                           self.gadget_provider,
                           self.gadget_gatewayid)
        return result

####################################################################################################
# Class representing connection with Gadget Core
# The SDK will use this class to establish TCP connection with Garuda Core and send/receive data to/from Garuda Core
# This class is for internal use of the SDK
####################################################################################################
class GarudaConnection(threading.Thread):

    running = False
    handle_read = lambda self, message: None

    # Constructor for the Connection class
    def __init__(self, addr=_GAURDA_ADDR):
        threading.Thread.__init__(self)
        self.addr = addr
        self.socket = self.open_socket()
        self.read_buffer = ''

    # The connection thread execution method
    def run(self):
        self.running = True
        while self.running:
            self.read()
            time.sleep(0.01)

    # Connection creation handler method 
    def open_socket(self):
        try:
            return socket.create_connection(self.addr)
        except socket.error:
            raise CannotConnect()

    # Connection termination handler method
    def close_socket(self):
        self.running = False
        if self.socket:
            self.socket.shutdown(2)
            self.socket.close()
            self.socket = None

    # Handler method for sending data over the connection
    def send(self, data):
        if not self.socket:
            raise ConnectTerminated()
        try:
            self.socket.sendall(bytes(data, 'UTF-8'))
        except socket.error as what:
            if what[0] == 10054 or what[0] == 9:
                raise ConnectTerminated()
            else:
                raise CannotSend(what[0], what[1])

    # Handler method for listening data over the connection
    def read(self):
        read = ''
        try:
            read = self.socket.recv(_BUFFER_SIZE)
            self.read_buffer = self.read_buffer + read.decode('utf-8')
        except socket.error:
            return
        index = self.read_buffer.find('\n')
        if index >= 0:
            message = self.read_buffer[:index+1]
            if message.strip() == _ID_TERMINATE_GADGET_REQ:
                self.handle_read(message.strip())
                sys.exit(0)
            elif message.strip() == '':
                pass
            else:
                self.handle_read(message)
            self.read_buffer = self.read_buffer[index+1:]

    # Method that registers the 'Read Callback' listener of the SDK with to the GarudaConnection class
    def bind(self, func):
        self.handle_read = func

####################################################################################################
# Class representing the SDK
# This class handles all types of communication with Garuda Core and provides API for different Garuda related action
# Gadget implementation should instantiate this class for intended Garuda operations
####################################################################################################
class GarudaClientBackend:

    _compatible_gadget_list = []

    _listner_callback = lambda self,  message_id, error_code, param: None
    display_log = lambda self, log_message: None

    # Constructor for the SDK class
    def __init__(self, gadget_name, gadget_id):
        self.gadget_name = gadget_name
        self.gadget_id = gadget_id
        self.connection = None
        self.initialized = False

    # This method establishes connection with Garuda core (using the connection class of the SDK) and activates the gadget
    # Gadget implementation MUST invoke this method once the SDK is instantiated and callback method is registered
    def initialize(self):
        if self.initialized:
            return
        try:
            self.connection = GarudaConnection()
        except CannotConnect:
            self._listner_callback(ID_CONNECTION_TERMINATED, None, None)
            return
        self._listner_callback(ID_CONNECTION_NOT_INITIALIZED, None, None)
        self.connection.bind(self.handle_read)
        self.connection.setDaemon(True)
        self.connection.start()
        self.activate_gadget()
        self.initialized = True

    # This method gives the initialization status of the SDK
    def is_initialized(self):
        return self.initialized

    # This method registers the callback listener (provided by the gadget implementation) to the SDK
    # Gadget implementation MUST invoke this method once the SDK is instantiated
    def add_lisenter(self, event):
        self._listner_callback = event

    # API for sending 'Gadget Activation' request to Garuda Core
    # On receiving response from Garuda Core, the SDK invokes the callback listener with ID_ACTIVATE_GADGET_RESPONSE
    # Gadget implementation need not call this API as this step has already been executed during SDK initialization step
    # However, the callback listener method in the gadget implementation should handle the response message
    def activate_gadget(self):
        header = dict(id = _ID_ACTIVATE_GADGET_REQ,
                      version = _REQ_MSG_VERSION)
        body = dict(sourceGadgetName = self.gadget_name,
                    sourceGadgetID = self.gadget_id)
        self.handle_request(header, body)

    # API for sending 'Get Compatible Gadget List' request to Garuda Core
    # On receiving response from Garuda Core, the SDK invokes the callback listener with ID_GET_COMPATIBLE_GADGET_LIST_RESPONSE
    def request_compatible_gadget_list(self, file_extension='', file_format=''):
        if file_extension.strip() == '' or file_format.strip() == '':
            return
        header = dict(id=_ID_GET_COMPATIBLE_GADGET_LIST_REQ,
                      version=_REQ_MSG_VERSION)
        body = dict(fileExtension=file_extension,
                    fileFormat=file_format,
                    sourceGadgetName=self.gadget_name,
                    sourceGadgetID=self.gadget_id)
        self.handle_request(header, body)

    # API for sending 'Send Notification To Core' request to Garuda Core
    # Note that for this request, the SDK does not provide any response message, i.e., there is no callback listener invocation as response
    def send_notification_to_core(self, gadget, notify_type, message):
        header = dict(id=_ID_SEND_NOTIFICATION_TO_CORE_REQ,
                      version=_REQ_MSG_VERSION)
        body = dict(sourceGadgetName=gadget.gadget_name,
                    sourceGadgetID=gadget.gadget_id,
                    type=notify_type,
                    message=message)
        self.handle_request(header, body)

    # API for sending 'Send Data To Gadget' request to Garuda Core
    # On receiving response from Garuda Core, the SDK invokes the callback listener with ID_SEND_DATA_GADGET_RESPONSE
    def send_data_to_gadget(self, data, target_gadget_name, target_gadget_id, is_stream=False):
        header = dict(id=_ID_SEND_DATA_TO_GADGET_REQ,
                      version=_REQ_MSG_VERSION)
        if is_stream:
            isst = True
        else:
            isst = False
        body = dict(data=data,
                    sourceGadgetName=self.gadget_name,
                    sourceGadgetID=self.gadget_id,
                    targetGadgetName=target_gadget_name,
                    targetGadgetID=target_gadget_id,
                    isStream=isst)
        self.handle_request(header, body)

    # Handler method for creating and sending request message to Garuda Core
    # For internal use of the SDK
    def handle_request(self, header, body):
        request_message = ""
        try:
            request_message = json.dumps(dict(header=header, body=body))
        except Exception as what:
            param = dict(message=what)
            self._listner_callback(ID_JSON_DUMPS_ERROR, None, param)
            return
        self.send_message(request_message)

    # API that returns the 'Compatible Gadget list' received from Garuda Core
    # The method returns an array of instances of the class Gadget
    def get_compatible_gadget_list(self):
        return self._compatible_gadget_list

    # Handler method retrieving message id received from Garuda Core
    # For internal use of the SDK
    def get_data_id(self, data):
        try:
            json_data = json.loads(data)
            return json_data["header"]["id"]
        except:
            return None

    # Method for sending message to Garuda Core (using the connection class of the SDK)
    # For internal use of the SDK
    def send_message(self, message):
        message_id = self.get_data_id(message)
        self.display_log(message_id+": ")
        if not self.connection:
            self._listner_callback(ID_CONNECTION_TERMINATED, None, None)
            return
        else:
            pass
        try:
            if message.endswith('\n'):
                self.connection.send(message)
            else:
                message = message + '\n'
                self.connection.send(message)
            json_message = json.loads(message)
            self.display_log(json.dumps(json_message, indent=4))
            self.print_log(message)
        except CannotSend as cannot_send_error:
            param = dict(message=cannot_send_error)
            self._listner_callback(ID_CONNECTION_TERMINATED, None, param)
        except ConnectTerminated as what:
            param = dict(message=what)
            self._listner_callback(ID_CONNECTION_TERMINATED, None, param)
        except Exception:
            pass

    # The 'Read Callback' method registered to the connection class of the SDK
    # This method handles the messages received from Garuda Core
    # For internal use of the SDK
    def handle_read(self, data):
        self.print_log(data)

        # Handle stop message
        if data == _ID_TERMINATE_GADGET_REQ:
            param = dict(message=MSG_REMOTE_HOST_CLOSED)
            self._listner_callback(ID_CONNECTION_TERMINATED, None, param)
            return

        data_id = self.get_data_id(data)

        json_message = json.loads(data)
        self.display_log(data_id+": ")
        self.display_log(json.dumps(json_message, indent=4))

        if data_id == _ID_ACTIVATE_GADGET_RESP:
            self.parser_activate_gadget(data)
        elif data_id == _ID_GET_COMPATIBLE_GADGET_LIST_RESP:
            self.parser_compatible_gadget_list(data)
        elif data_id == _ID_SEND_DATA_TO_GADGET_RESP:
            self.parser_send_data_to_gadget(data)
        elif data_id == _ID_LOAD_DATA_REQ:
            self.parser_load_data(data)
        elif data_id == _ID_LOAD_GADGET_REQ:
            self.parser_load_gadget(data)
        elif data_id == _ID_SEND_NOTIFICATION_TO_GADGET_REQ:
            self.parser_send_notification_to_gadget(data)
        else:
            pass

    # Handler method for the 'Activation Response' message
    # On success, the method invokes the callback listener of the gadget with ID_ACTIVATE_GADGET_RESPONSE
    # For internal use of the SDK
    def parser_activate_gadget(self, data):
        try:
            json_data = json.loads(data)
            response_code = json_data["body"]["result"] 
            if response_code != RESPCODE_SUCCESS:
                self._listner_callback(ID_ACTIVATE_GADGET_RESPONSE, response_code, None)
        except Exception as what:
            param = dict(message=what)
            self._listner_callback(ID_JSON_PARSE_ERROR, None, param)

    # Handler method for the 'Compatible Gadget List Response' message
    # On success, the method invokes the callback listener of the gadget with ID_GET_COMPATIBLE_GADGET_LIST_RESPONSE
    # For internal use of the SDK
    def parser_compatible_gadget_list(self, data):
        self._compatible_gadget_list = []
        gadgets = []
        response_code = None
        try:
            json_data = json.loads(data)
            gadgets = json_data["body"]["gadgets"]
            if not gadgets:
                gadgets = []
            response_code = json_data["body"]["result"]
        except Exception as what:
            param = dict(message=what)
            self._listner_callback(ID_JSON_PARSE_ERROR, None, param)
            return
        for gadget in gadgets:
            gdgt = Gadget(gadget.get("name", None),
                          gadget.get("ID", None),
                          gadget.get("iconPath", None),
                          gadget.get("provider", None),
                          gadget.get("gateway_id", None))
            self._compatible_gadget_list.append(gdgt)
        self._listner_callback(ID_GET_COMPATIBLE_GADGET_LIST_RESPONSE, response_code, None)

    # Handler method for the 'Send Data To Gadget Response' message
    # On success, the method invokes the callback listener of the gadget with ID_SEND_DATA_GADGET_RESPONSE
    # For internal use of the SDK
    def parser_send_data_to_gadget(self, data):
        try:
            json_data = json.loads(data)
            response_code = json_data["body"]["result"]
            if response_code == RESPCODE_SUCCESS:
                gadget = Gadget(json_data["body"]["targetGadgetName"],
                                json_data["body"]["targetGadgetID"],
                                None,
                                None,
                                None)
                self._listner_callback(ID_SEND_DATA_GADGET_RESPONSE, response_code, gadget)
            else:
                self._listner_callback(ID_SEND_DATA_GADGET_RESPONSE, response_code, None)
        except Exception as what:
            param = dict(message=what)
            self._listner_callback(ID_JSON_PARSE_ERROR, None, param)

    # Handler method for the 'Load Data' request from Garuda Core
    # On success, the method invokes the callback listener of the gadget with any of -
    # ID_LOAD_DATA_STREAM_REQUEST when the loadable data is stream data
    # ID_LOAD_DATA_REQUEST when the loadable data is not streamed
    # For internal use of the SDK
    def parser_load_data(self, data):
        try:
            json_data = json.loads(data)
            gadget = Gadget(json_data["body"]["originGadgetName"],
                            json_data["body"]["originGadgetID"],
                            None,
                            None,
                            None)
            is_stream = json_data["body"]["isStream"]
            gadget_data = json_data["body"]["data"]
            param = dict(gadget=gadget, data=gadget_data)
            if is_stream:
                self._listner_callback(ID_LOAD_DATA_STREAM_REQUEST, None, param)
            else:
                self._listner_callback(ID_LOAD_DATA_REQUEST, None, param)
        except Exception as what:
            param = dict(message=what)
            self._listner_callback(ID_JSON_PARSE_ERROR, None, param)

    # Handler method for the 'Load Gadget' request from Garuda Core
    # On success, the method invokes the callback listener of the gadget with ID_LOAD_GADGET_REQUEST
    # For internal use of the SDK
    def parser_load_gadget(self, data):
        try:
            json_data = json.loads(data)
            gadget = Gadget(json_data["body"]["loadableGadgetName"],
                            json_data["body"]["loadableGadgetID"],
                            None,
                            None,
                            None)
            loadable_gadget_source_path = json_data["body"]["loadableGadgetSourcePath"]
            param = dict(gadget=gadget, path=loadable_gadget_source_path)
            self._listner_callback(ID_LOAD_GADGET_REQUEST, None, param)
        except Exception:
            pass

    # Handler method for the 'Send notification To Gadget' request from Garuda Core
    # On success, the method invokes the callback listener of the gadget with ID_SEND_NOTIFICATION_TO_GADGET_REQUEST
    # For internal use of the SDK
    def parser_send_notification_to_gadget(self, data):
        try:
            json_data = json.loads(data)
            targetGadgetName = json_data["body"]["targetGadgetName"]
            targetGadgetId = json_data["body"]["targetGadgetID"]
            if targetGadgetName == self.gadget_name and targetGadgetId == self.gadget_id:
                gadget = Gadget(targetGadgetName,
                                targetGadgetId,
                                None,
                                None,
                                None)
                notify_type = json_data["body"]["type"]
                message = json_data["body"]["message"]
                param = dict(message=message, gadget=gadget)
                self._listner_callback(ID_SEND_NOTIFICATION_TO_GADGET_REQUEST, notify_type, param)
        except Exception as what:
            param = dict(message=what)
            self._listner_callback(ID_JSON_PARSE_ERROR, None, param)

    # Handler method for printing log message
    # For internal use of the SDK
    def print_log(self, message):
        # Print Log
        logc = []
        log_title = self.get_data_id(message)

        if not log_title:
            print ('END')
            return

        log_left = log_right = int((80-len(log_title)) / 2)
        logc.append('=' * (log_left-1) + ' ' + log_title + ' ' + '=' * (log_right-1))
        content = json.dumps(json.loads(message), indent=4)
        logc.append(content)
        logc.append('=' * 80)
        logc.append('\n')

    # API for sending 'Load Data Response' to Garuda Core
    # Gadget implementation should invoke this API for sending response of ID_LOAD_DATA_REQUEST and ID_LOAD_DATA_STREAM_REQUEST
    # response_code:
    #        200 - Success.
    #        415 - Data Resource Not found.
    #        500 - Internal Error.
    #        508 - Incompatible data type.
    #        512 - Incomplete Request parameters.
    def response_load_data(self, target_gadget_name, target_gadget_id, response_code): 
        if not response_code:
            return
        header = dict(id=_ID_LOAD_DATA_RESP,
                      version=_RESP_MSG_VERSION)
        body = dict(result=response_code,
                    sourceGadgetName=self.gadget_name,
                    sourceGadgetID=self.gadget_id,
                    originatorID=self.gadget_id,
                    targetGadgetName=target_gadget_name,
                    targetGadgetID=target_gadget_id)
        self.handle_request(header, body)

    # API for sending 'Load Gadget Response' to Garuda Core
    # Gadget implementation should invoke this API for sending response of ID_LOAD_GADGET_REQUEST
    # response_code:
    #        200 - Success.
    #        500 - Internal Error.
    #        512 - Incomplete Request parameters.
    def response_load_gadget(self, loaded_gadget_name, loaded_gadget_id, response_code):
        if not response_code:
            return
        header = dict(id=_ID_LOAD_GADGET_RESP,
                      version=_RESP_MSG_VERSION)
        body = dict(result=response_code,
                    sourceGadgetName=self.gadget_name,
                    sourceGadgetID=self.gadget_id,
                    originatorID=self.gadget_id,
                    loadedGadgetName=loaded_gadget_name,
                    loadedGadgetID=loaded_gadget_id)
        self.handle_request(header, body)

    # API for sending 'Send Notification to Gadget Response' to Garuda Core
    # Gadget implementation should invoke this API for sending response of ID_SEND_NOTIFICATION_TO_GADGET_REQUEST
    # response_code:
    #        200 - Success.
    #        500 - Internal Error.
    #        501 - Unable to parse JSON.
    #        503 - Invalid Request Message JSON.
    #        512 - Incomplete Request parameters.
    #        513 - Invalid Notification Code.
    def response_send_notification_to_gadget(self, source_gadget_name, source_gadget_id, response_code):
        if not response_code:
            return
        header = dict(id=_ID_SEND_NOTIFICATION_TO_GADGET_RESP,
                      version=_RESP_MSG_VERSION)
        body = dict(result=response_code,
                    sourceGadgetName=source_gadget_name,
                    sourceGadgetID=source_gadget_id)
        self.handle_request(header, body)

    # API for terminating connection with Garuda Core (using the connection class of the SDK)
    def stop_backend(self):
        if self.connection:
            self.connection.close_socket()
            self.connection = None
        self.initialized = False
        sys.exit()
