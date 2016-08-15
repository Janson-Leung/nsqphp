<?php
namespace nsqphp\Connection;

use nsqphp\Exception\SocketException;
use nsqphp\Exception\ConnectionException;

/**
 * Represents a single connection to a single NSQD server
 */
class SocketConnection implements ConnectionInterface {
    /**
     * Hostname
     * 
     * @var string
     */
    private $hostname;
    
    /**
     * Port number
     * 
     * @var integer
     */
    private $port;
    
    /**
     * Connection timeout - in seconds
     * 
     * @var float
     */
    private $connectionTimeout;
    
    /**
     * Read/write timeout - sec/usec
     * 
     * @var array
     */
    private $readWriteTimeout;

    /**
     * Read wait timeout - in whole seconds
     * 
     * @var integer
     */
    private $readWaitTimeoutSec;

    /**
     * Read wait timeout - in whole microseconds
     * 
     * (to be added to the whole seconds above)
     * 
     * @var integer
     */
    private $readWaitTimeoutUsec;
    
    /**
     * Non-blocking mode?
     * 
     * @var boolean
     */
    private $nonBlocking;
    
    /**
     * Optional on-connect callback
     * 
     * @var callable|NULL
     */
    private $connectCallback;

    /**
     * Socket handle
     * 
     * @var Resource|NULL
     */
    private $socket = NULL;
    
    /**
     * Constructor
     * 
     * @param string $hostname Default localhost
     * @param integer $port Default 4150
     * @param float $connectionTimeout In seconds (no need to be whole numbers)
     * @param float $readWriteTimeout Socket timeout during active read/write
     *      In seconds (no need to be whole numbers)
     * @param float $readWaitTimeout How long we'll wait for data to become
     *      available before giving up (eg; duirng SUB loop)
     *      In seconds (no need to be whole numbers)
     * @param boolean $nonBlocking Put socket in non-blocking mode
     * @param callable|NULL $connectCallback Optional on-connect callback (will
     *      be called whenever we establish a connection)
     */
    public function __construct(
            $hostname = 'localhost',
            $port = NULL,
            $connectionTimeout = 3,
            $readWriteTimeout = 3,
            $readWaitTimeout = 15,
            $nonBlocking = FALSE,
            $connectCallback = NULL
            ) {
        $this->hostname = $hostname;
        $this->port = $port ? $port : 4150;
        $this->connectionTimeout = $connectionTimeout;

        $_readWriteTimeout = floor($readWriteTimeout);
        $this->readWriteTimeout = array(
            'sec' => $_readWriteTimeout,
            'usec' => ($readWriteTimeout - $_readWriteTimeout) * 1000000
        );

        $this->readWaitTimeoutSec = floor($readWaitTimeout);
        $this->readWaitTimeoutUsec = ($readWaitTimeout - $this->readWaitTimeoutSec) * 1000000;

        $this->nonBlocking = (bool)$nonBlocking;
        $this->connectCallback = $connectCallback;
    }
    
    /**
     * Wait for readable
     * 
     * Waits for the socket to become readable (eg: have some data waiting)
     * 
     * @return boolean
     */
    public function isReadable() {
        $read = array($socket = $this->getSocket());
        $readable = @socket_select($read, $null, $null, $this->readWaitTimeoutSec, $this->readWaitTimeoutUsec);
        return $readable ? TRUE : FALSE;
    }
    
    /**
     * Read from the socket exactly $length bytes
     *
     * @param integer $length How many bytes to read
     * 
     * @return string Binary data
    */
    public function read($length) {
        $read = 0;
        $parts = [];

        while ($read < $length) {
            $data = @socket_read($this->getSocket(), $length - $read, PHP_BINARY_READ);
            if ($data === false) {
                $this->error('Failed to read data from ' . $this->__toString());
            }
            $read += strlen($data);
            $parts[] = $data;
        }

        return implode($parts);
    }

    /**
     * Reconnect and return the socket
     *
     * @return Resource the socket
     */
    public function reconnect() {
        if (is_resource($this->socket)) {
            @socket_shutdown($this->socket);
            @socket_close($this->socket);
        }
        $this->socket = NULL;
        return $this->getSocket();
    }

    /**
     * Write to the socket.
     *
     * @param string $data The data to write
     */
    public function write($data) {
        $written = 0;
        $length = strlen($data);

        while($written < $length) {
            $fwrite = @socket_write($this->getSocket(), substr($data, $written));
            if ($fwrite === false) {
                $this->error('Failed to write buffer to ' . $this->__toString());
            }

            $written += $fwrite;
        }
    }

    /**
     * Fail with socket error
     *
     * @param string $msg
     *
     * @throws SocketException
     */
    private function error($msg) {
        $errmsg = @socket_strerror($errno = socket_last_error($this->socket));
        throw new SocketException("{$errmsg} -> {$msg}", $errno);
    }

    /**
     * Get socket handle
     * 
     * @return Resource The socket
     */
    public function getSocket() {
        if ($this->socket === NULL) {
            $this->socket = @socket_create(AF_INET, SOCK_STREAM, SOL_TCP);

            if ($this->socket === false) {
                throw new SocketException('Failed to create socket to ' . $this->__toString());
            }

            if (@socket_connect($this->socket, $this->hostname, $this->port) === false) {
                throw new SocketException('Failed to connect socket to ' . $this->__toString());
            }

            if (@socket_set_option($this->socket, SOL_SOCKET, SO_RCVTIMEO, $this->readWriteTimeout) === false) {
                $this->error('Failed to set socket stream recv timeout option on ' . $this->__toString());
            }

            if (@socket_set_option($this->socket, SOL_SOCKET, SO_SNDTIMEO, $this->readWriteTimeout) === false) {
                $this->error('Failed to set socket stream send timeout option on ' . $this->__toString());
            }

            if ($this->nonBlocking) {
                socket_set_nonblock($this->socket);
            }

            // on-connection callback
            if ($this->connectCallback !== NULL) {
                call_user_func($this->connectCallback, $this);
            }
        }

        return $this->socket;
    }
    
    /**
     * To string (for debug logging)
     * 
     * @return string
     */
    public function __toString() {
        return "{$this->hostname}:{$this->port}";
    }
}
