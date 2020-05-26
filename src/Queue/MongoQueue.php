<?php

namespace Moloquent\Queue;

use Carbon\Carbon;
use Exception;
use Illuminate\Database\Connection;
use Illuminate\Queue\DatabaseQueue;
use Illuminate\Queue\Jobs\DatabaseJob;
use MongoDB;
use MongoDB\Driver;
use Log;

class MongoQueue extends DatabaseQueue
{
    /** @var MongoDB\Client */
    private $client;

    private $databaseName;

    /**
     * @param \Illuminate\Database\Connection $database
     * @param string $table
     * @param string $default
     * @param int $expire
     */
    public function __construct(Connection $database, $table, $default = 'default', $expire = 60)
    {
        parent::__construct($database, $table, $default, $expire);
        $dsn = $database->getConfig('dsn');

        $options = [
            "journal" => true,
            "w" => MongoDB\Driver\WriteConcern::MAJORITY
        ];

        $driverOptions = [];

        $this->client = new MongoDB\Client($dsn, $options, $driverOptions);
        $this->databaseName = $database->getConfig('database');
    }

    /**
     * Pop the next job off of the queue.
     *
     * @param string $queue
     *
     * @return \Illuminate\Contracts\Queue\Job|null
     */
    public function pop($queue = null)
    {
        $queue = $this->getQueue($queue);

        // Atomic operation to find and update the job in the same instant
        if ($job = $this->getNextAvailableJobAndMarkAsReserved($queue)) {
            return new DatabaseJob(
                $this->container, $this, $job, $this->connectionName, $queue
            );
        }
    }

    /**
     * Delete a reserved job from the queue.
     *
     * @param string $queue
     * @param string $id
     *
     * @return void
     */
    public function deleteReserved($queue, $id)
    {
        try {
            $this->client->selectCollection($this->databaseName, $this->table)->deleteOne(
                ['_id' => $id],
                ['$isolated' => 1]
            );
        } catch (Exception $e) {
            Log::error('Error while selecting mongo collection: ' . $e->getMessage());
        }
    }

    /**
     *
     * @param $queue
     */
    protected function getNextAvailableJobAndMarkAsReserved($queue)
    {
        $expiration = Carbon::now()->subSeconds($this->retryAfter)->getTimestamp();

        $job = $this->client->selectCollection($this->databaseName, $this->table)->findOneAndUpdate(
            [
                'queue' => $this->getQueue($queue),
                '$or' => [
                    [
                        'reserved_at' => null,
                        'available_at' => ['$lte' => $this->currentTime()]
                    ],
                    [
                        'reserved_at' => ['$lte' => $expiration]
                    ]
                ]
            ],
            [
                '$set' => ['reserved_at' => $this->currentTime()],
                '$inc' => ['attempts' => 1]
            ],
            [
                'sort' => ['_id' => 1],
                'returnDocument' => MongoDB\Operation\FindOneAndUpdate::RETURN_DOCUMENT_AFTER,
                '$isolated' => 1
            ]
        );

        if ($job) {
            $this->database->commit();
            $job = (object)$job;
            $job->id = $job->_id;
        }

        return $job ?: null;
    }

    /**
     * Get the next available job for the queue.
     *
     * @param string|null $queue
     *
     * @return \StdClass|null
     */
    protected function getNextAvailableJob($queue)
    {
        $expiration = Carbon::now()->subSeconds($this->retryAfter)->getTimestamp();

        $job = $this->client->selectCollection($this->databaseName, $this->table)->findOne(
            [
                'queue' => $this->getQueue($queue),
                '$or' => [
                    [
                        'reserved_at' => null,
                        'available_at' => ['$lte' => $this->currentTime()]
                    ],
                    [
                        'reserved_at' => ['$lte' => $expiration]
                    ]
                ]
            ],
            ['sort' => ['_id' => 1]]);

        if ($job) {
            $job = (object)$job;
            $job->id = $job->_id;
        }

        return $job ?: null;
    }

    /**
     * Mark the given job ID as reserved.
     *
     * @param \stdClass $job
     *
     * @return \stdClass
     */
    protected function markJobAsReserved($job)
    {
        $job->attempts = $job->attempts + 1;
        $job->reserved_at = $this->currentTime();

        try {
            $this->client->selectCollection($this->databaseName, $this->table)->updateOne(
                ['_id' => $job->id],
                [
                    '$set' => [
                        'reserved_at' => $job->reserved_at,
                        'attempts' => $job->attempts
                    ]
                ],
                ['$isolated' => 1]
            );

            $this->database->commit();
        } catch (Exception $e) {
            Log::error('Error while marking job as reserved: ' . $e->getMessage());
        }
    }
}
