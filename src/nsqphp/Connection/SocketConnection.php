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
     * @var callable|null
     */
    private $connectCallback;

    /**
     * Socket handle
     * 
     * @var Resource|null
     */
    private $socket = null;
    
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
     * @param callable|null $connectCallback Optional on-connect callback (will
     *      be called whenever we establish a connection)
     */
    public function __construct(
            $hostname = 'localhost',
            $port = null,
            $connectionTimeout = 3,
            $readWriteTimeout = 3,
            $readWaitTimeout = 15,
            $nonBlocking = false,
            $connectCallback = null
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

        return $readable ? true : false;
    }
    
    /**
     * Read from the socket exactly $length bytes
     *
     * @param integer $length How many bytes to read
     * 
     * @return string Binary data
    */
    public function read($length) {
        $null = null;
        $socket_read = array($socket = $this->getSocket());

        $surplus = $length;
        $data = '';
        while ($surplus > 0) {
            $readable = socket_select($socket_read, $null, $null, $this->readWriteTimeout['sec'], $this->readWriteTimeout['usec']);

            if ($readable > 0) {
                $fread = @socket_read($socket, $surplus, PHP_BINARY_READ);

                if ($fread === false) {
                    $this->error('Failed to read data from ' . $this->getDomain());
                } elseif ($fread == '') {
                    $this->error('NsqSocket read 0 bytes from ' . $this->getDomain(), SOCKET_ECONNRESET);
                }
            } elseif ($readable === 0) {
                throw new SocketException(
                    sprintf('Timed out reading %d bytes from %s after %d seconds and %d microseconds', $surplus,
                        $this->getDomain(), $this->readWriteTimeout['sec'], $this->readWriteTimeout['usec'])
                );
            } else {
                throw new SocketException(
                    sprintf('Could not read %d bytes from %s', $surplus, $this->getDomain())
                );
            }

            $data .= $fread;
            $surplus -= strlen($fread);
        }

        return $data;
    }

    /**
     * Write to the socket.
     *
     * @param string $data The data to write
     */
    public function write($data) {
        $null = null;
        $write = array($socket = $this->getSocket());

        $surplus = strlen($data);
        while($surplus > 0) {
            $writable = socket_select($null, $write, $null, $this->readWriteTimeout['sec'], $this->readWriteTimeout['usec']);

            if ($writable > 0) {
                $written = @socket_write($this->getSocket(), $data);

                if ($written === false) {
                    $this->error('Failed to write to ' . $this->getDomain());
                }
            } elseif ($writable === 0) {
                throw new SocketException(
                    sprintf('Timed out writing %d bytes to %s after %d seconds and %d microseconds', $surplus,
                        $this->getDomain(), $this->readWriteTimeout['sec'], $this->readWriteTimeout['usec'])
                );
            } else {
                throw new SocketException(
                    sprintf('Could not write %d bytes to %s', $surplus, $this->getDomain())
                );
            }

            $data = substr($data, $written);
            $surplus -= $written;
        }
    }

    /**
     * Get socket handle
     * 
     * @return Resource The socket
     */
    public function getSocket() {
        if ($this->socket === null) {
            $this->socket = @socket_create(AF_INET, SOCK_STREAM, SOL_TCP);

            if ($this->socket === false) {
                throw new SocketException('Failed to create socket');
            }

            if (@socket_set_option($this->socket, SOL_SOCKET, SO_RCVTIMEO, $this->readWriteTimeout) === false) {
                $this->error('Failed to set socket recv timeout option');
            }

            if (@socket_set_option($this->socket, SOL_SOCKET, SO_SNDTIMEO, $this->readWriteTimeout) === false) {
                $this->error('Failed to set socket send timeout option');
            }

            if (@socket_connect($this->socket, $this->hostname, $this->port) === false) {
                throw new ConnectionException('Failed to connect socket to ' . $this->getDomain());
            }

            if ($this->nonBlocking) {
                socket_set_nonblock($this->socket);
            }

            // on-connection callback
            if ($this->connectCallback !== null) {
                call_user_func($this->connectCallback, $this);
            }
        }

        return $this->socket;
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

        $this->socket = null;
        return $this->getSocket();
    }

    /**
     * Get the domain that this socket is connected to
     *
     * @return string
     */
    public function getDomain() {
        return "{$this->hostname}:{$this->port}";
    }

    /**
     * To string (for debug logging)
     *
     * @return string
     */
    public function __toString() {
        return $this->getDomain();
    }

    /**
     * Fail with socket error
     *
     * @param string $msg
     * @param int $errno
     *
     * @throws SocketException
     */
    private function error($msg, $errno = 0) {
        $errno = $errno ?: socket_last_error($this->socket);
        $errmsg = @socket_strerror($errno);

        throw new SocketException("{$errmsg} -> {$msg}", $errno);
    }
}
