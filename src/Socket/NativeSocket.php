<?php

namespace Pheanstalk\Socket;

use Pheanstalk\Exception;
use Pheanstalk\Socket;

/**
 * A Socket implementation around a fsockopen() stream.
 *
 * @author  Paul Annesley
 * @package Pheanstalk
 * @license http://www.opensource.org/licenses/mit-license.php
 */
class NativeSocket implements Socket
{
	/** @var resource */
    private $socket;
	/** @var int $timeout in seconds*/
	private $timeout;

	/**
	 * NativeSocket constructor.
	 * @param $host
	 * @param $port
	 * @param $timeout
	 * @throws \Exception
	 * @throws Exception\ConnectionException
	 * @throws Exception\SocketException
	 */
	public function __construct($host, $port, $timeout)
    {
		$this->timeout = $timeout;
		
	    if (!\extension_loaded('sockets')) {
		    throw new \Exception('Sockets extension not found');
	    }
	    $this->socket = \socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
	    if (false === $this->socket) {
		    $this->throwException();
	    }
		$timeval = [
			'sec' => $timeout,
			'usec' => 0,
		];
	    \socket_set_option($this->socket, SOL_TCP, SO_KEEPALIVE, 1);
		\socket_set_option($this->socket, SOL_SOCKET, SO_SNDTIMEO, $timeval);
		\socket_set_option($this->socket, SOL_SOCKET, SO_RCVTIMEO, $timeval);
	    \socket_set_block($this->socket);
	    $addresses = \gethostbynamel($host);
	    if (false === $addresses) {
		    throw new Exception\ConnectionException(0, "Could not resolve hostname $host");
	    }
	    if (!\socket_connect($this->socket, $addresses[0], $port)) {
		    $error = \socket_last_error($this->socket);
		    throw new Exception\ConnectionException($error, \socket_strerror($error));
	    };
    }

	/**
	 * Writes data to the socket
	 * @param string $data
	 * @throws Exception\SocketException
	 */
	public function write($data)
	{
		$this->checkClosed();

		if (!is_string($data)){
			return;
		}

		while ('' !== $data) {
			$written = \socket_write($this->socket, $data, \strlen($data));
			if (false === $written) {
				$this->throwException();
			}
			$data = (string) \substr($data, $written);
		}
	}

	/**
	 * Reads up to $length bytes from the socket
	 * @param int $length
	 * @return string
	 * @throws Exception\SocketException
	 */
	public function read($length)
	{
		$this->checkClosed();
		$buffer = '';
		while (\mb_strlen($buffer, '8BIT') < $length) {
			$result = \socket_read($this->socket, $length - mb_strlen($buffer, '8BIT'));
			if (false === $result) {
				$this->throwException();
			}
			$buffer .= $result;
		}
		return $buffer;
	}

	/**
	 * @param int|null $length
	 * @return string
	 * @throws Exception\SocketException
	 */
	public function getLine($length = null)
	{
		$this->checkClosed();
		$buffer = '';

		// Reading stops at \r or \n. In case it stopped at \r we must continue reading.
		do {
			$line = \socket_read($this->socket, $length ?: 1024, PHP_NORMAL_READ);
			if (false === $line) {
				$this->throwException();
			}

			if ('' === $line) {
				break;
			}

			$buffer .= $line;
		} while ('' !== $buffer && "\n" !== $buffer[\strlen($buffer) - 1]);

		return \rtrim($buffer);
	}

	/**
	 * @throws Exception\SocketException
	 */
	public function disconnect()
	{
		$this->checkClosed();
		\socket_close($this->socket);
		unset($this->socket);
	}

	/**
	 * @throws Exception\SocketException
	 */
	private function throwException()
	{
		$error = \socket_last_error($this->socket);
		throw new Exception\SocketException(\socket_strerror($error), $error);
	}

	/**
	 * @throws Exception\SocketException
	 */
	private function checkClosed()
	{
		if (null === $this->socket) {
			throw new Exception\SocketException('The connection was closed');
		}
	}

	//
}
