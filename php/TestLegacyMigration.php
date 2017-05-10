<?php

namespace CompanyTech\DataWarehouse\Migrations;

use CompanyTech\Core\Entity\Client\StudentYearGrade;
use CompanyTech\Core\Entity\Client\UploadedFile;
use CompanyTech\DataWarehouse\Entity\Test;
use CompanyTech\DataWarehouse\Entity\TestImport;
use CompanyTech\DataWarehouse\Mappers\TestLegacyMigrationMapper;
use CompanyTech\DataWarehouse\Repository\TestsRepository;
use App\Command\Connections\DoctrineManager;
use App\Queue\Jobs\TestImportJob;
use App\Queue\AppQueueManager;
use App\Queue\QueuePayload;
use Zend\Console\Adapter\AdapterInterface as Console;

/**
 *
 * @author schmidty
 * @date Aug 15, 2016
 */
class TestLegacyMigration
{

    /**
     * app entity repo
     *
     * @var string
     */
    private $defaultEm;

    /**
     * optional email address for finished migration report
     *
     * @var string
     */
    public $email = 'info@test-company.com';

    /**
     * All client databases
     *
     * @var array
     */
    private $subscriptions;

    /**
     * S3 URL configurable value
     *
     * @var string
     */
    private $s3Uploader;

    /**
     * S3 URL configurable value
     *
     * @var string
     */
    private $remoteBucketPath;

    /**
     * Doctrine Created from DoctrineManager -- something different than Doctrine's manager
     *
     * @var DoctrineManager
     */
    private $doctrine;

    /**
     * The client database name value
     *
     * @var integer
     */
    private $database;

    /**
     * The client database subscription id value
     *
     * @var integer
     */
    private $subscriptionId;

    /**
     * The filepath for the datafile
     *
     * @var string
     */
    private $filepath = '/tmp';

    /**
     * The configured filename for new datafile
     *
     * @var string
     */
    private $builtFilename;

    /**
     * Configurable custom filename for new data file
     *
     * @var string
     */
    private $filename;

    /**
     * The path and filename for the new datafile
     *
     * @var string
     */
    private $filenamepath;

    /**
     * One specific test to migrate
     *
     * @var string
     */
    private $test;

    /**
     * NonMigrated Test Types
     *
     * @var array
     */
    private $testTypesToMigrate = [ 'core', 'added', 'avail' ];

    /**
     * NonMigrated Test Types
     *
     * @var array
     */
    private $migratedtestTypes = [ 'migrated', 'new' ];

    /**
     * Holds prefound migration sql per test
     *
     * @var array
     */
    private $testMigrationSql;

    /**
     * Mapper class to get test SQL select for migration data file
     *
     * @var TestLegacyMigrationMapper
     */
    private $testMigrationSqlMapper;

    /**
     * The handle reference for the filename
     *
     * @var resource
     */
    private $filehandle;

    /**
     * The subscription data
     *
     * @var integer
     */
    private $sqlScriptsDirectory = 'legacy-migration-scripts';

    /**
     * The SQL query being called from the SQL file
     *
     * @var boolean
     */
    private $showQuery;

    /**
     * Option to delete previous migration data before migrating again.
     *
     * @var boolean
     */
    private $remigrate;

    /**
     * Output for this command
     *
     * @var Outputinterface
     */
    public $console;

    /**
     * The xmanager object that contains all services
     *
     * @var object
     */
    public $xmanager;

    /**
     * @var TestsRepository
     */
    private $testsRepo;

    /**
     * constructor
     * @param object $request
     * @param object $xmanager
     * @throws \RuntimeException
     */
    public function __construct( $request, $xmanager )
    {
        $this->xmanager = $xmanager;
        $this->console = $this->xmanager->getServiceLocator()->get( 'console' );
//        $this->viewManager = $this->xmanager->getServiceLocator()->get( 'ViewManager' );
//
//        $this->viewManager->getExceptionStrategy()->setDisplayExceptions(false);
        $this->s3Uploader = $xmanager->get( 's3-uploader' );

        if( !$this->console instanceof Console ) {
            throw new \RuntimeException( 'Cannot obtain console adapter. Are we running in a console?' );
        }
        $this->getParameters( $request );
    }

    /**
     * Get the console parameters to set class variables
     * @param object $request
     * @throws \RuntimeException
     */
    protected function getParameters( $request )
    {
        // Set command-line params
        $this->database = $request->getParam( 'database', null );
        $this->showQuery = $request->getParam( 'show-query', false );

        $this->filename = $request->getParam( 'filename', $this->filename );
        $this->test = $request->getParam( 'test', null );
        $this->email = $request->getParam( 'email', $this->email );

        $this->remigrate = $request->getParam( 're-migrate', false );
    }

    /**
     * Execute method
     */
    public function execute()
    {
        // Get a list of clients to migrate test data for
        $this->setCommandVars();

        // Get a list of tests to run the migration on
        $this->getClientsForTests();
        $this->console->writeLine( "migration completed" );
    }

    /**
     * Set the class variables and current test data
     */
    protected function setCommandVars()
    {
        $this->doctrine = new DoctrineManager();
        $this->defaultEm = $this->doctrine->getDefaultEm();

        $this->subscriptions = $this->getAllSubscriptions();

        $this->sqlScriptsDirectory .= DIRECTORY_SEPARATOR;

        $this->testMigrationSqlMapper = new TestLegacyMigrationMapper();
    }

    /**
     * iterate through client database and tables array
     */
    private function getClientsForTests()
    {
        $this->console->writeLine( "starting legacy migration" .
            (isset( $this->database ) ?
                " on client {$this->database} " : " on all clients ") );

        foreach( $this->subscriptions as $subscription ) {
            $this->database = $subscription->getClientDatabase();
            $this->console->writeLine( "getting old test data for '" . $this->database . "' client database" );
            $this->doctrine->overrideClientEmByDatabaseName( $this->database );
            $this->subscriptionId = $subscription->getId();
            $tests = $this->getAllNonMigratedTests();
            if( $tests !== false ) {
                $this->migrateTests( $tests );
            }
        }
    }

    /**
     * Get either a single test or collection of tests to migrate
     * @return array
     */
    protected function getAllNonMigratedTests()
    {
        // This could be confusing... ugh?
        $this->testsRepo = $this->doctrine->getClientEm()->getRepository( 'CompanyTech\DataWarehouse\Entity\Tests' );
        $testRepo = $this->doctrine->getClientEm()->getRepository( 'CompanyTech\DataWarehouse\Entity\Test' );

        if( $this->test !== null ) {
            return $this->getOneTestByTestFilenameAndTypes( $this->testsRepo, $testRepo );
        } else {
            return $this->getByInTestTypes( $this->testsRepo, $testRepo );
        }
    }

    /**
     * Either returns a single test found or errors to error log and returns empty array
     * @param object $testsRepo
     * @param object $testRepo
     * @return array $testRepo
     */
    protected function getOneTestByTestFilenameAndTypes( $testsRepo, $testRepo )
    {
        $test = $testsRepo->findOneByTestFilename( $this->test );

        if( $test === null ) {
            echo sprintf(
                "Test %s does not exist for %s\n", $this->test, $this->database
            );
            return false;
        } elseif( in_array( $test->getTestType(), $this->migratedtestTypes ) && !$this->remigrate ) {
            echo sprintf(
                "Test %s has already been migrated for %s\n", $this->test, $this->database
            );
            return false;
        }
        $tests[] = $testRepo->findByTestKey( $this->test );
        return $tests;
    }

    /**
     * Returns a collection of tests found by test key that are valid configured test types
     * @param object $testsRepo
     * @param object $testRepo
     * @return array $testsFound
     */
    protected function getByInTestTypes( $testsRepo, $testRepo )
    {
        $testsFound = [];
        $tests = $testsRepo->findAllLegacyTests( true );
        foreach( $tests as $test ) {
            $testsFound[] = $testRepo->findByTestKey( $test->getTestFilename() );
        }
        return $testsFound;
    }

    /**
     * Get PDO results with query from client database
     * @param string $sql
     * @param array $params
     * @return array|false|null
     */
    private function queryClientData( $sql, $params = null )
    {
        $results = null;

        try {
            $pdo = $this->doctrine->getClientPdo();
            $pdo->setAttribute( \PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION );
            $stmt = $pdo->prepare( $sql );
            $stmt->execute( $params );
            $results = $stmt->fetchAll( \PDO::FETCH_ASSOC );
        } catch( \PDOException $ex ) {
            error_log( sprintf( "error %s  %s", $ex->getMessage(), $sql
            ) );
            $this->console->writeLine( sprintf( "PDO Error %s, check log...", $ex->getMessage() ) );
        }
        return $results;
    }

    /**
     * Get all client subscription objects into an array
     * @return array
     */
    protected function getAllSubscriptions()
    {
        $objects = null;
        $subscriptionConfigRepo = $this->defaultEm->getRepository( 'CompanyTech\Core\Entity\Core\SubscriptionConfiguration' );

        if( isset( $this->database ) ) {
            $objects = $subscriptionConfigRepo->findByClientName( $this->database );
        } else {
            $objects = $subscriptionConfigRepo->findAllAndOrderByDatabaseName();
        }

        if( empty( $objects ) ) {
            $this->console->writeLine( "no subscriptions found" );
        }
        return $objects;
    }

    /**
     *  Get the DateTime object formatted to Year,Month,Day,24hour,Min,Sec,MSecs
     * @return object
     */
    private function getDateTimeForFileName()
    {
        $dateObject = new \DateTime();
        return $dateObject->format( 'YmdHisu' );
    }

    /**
     * Map to the SQL script for each table (test) in client database
     * @param array $tests
     */
    private function migrateTests( $tests )
    {
        $this->testMigrationSql = [];

        foreach( $tests as $test ) {
            $sql = $this->getTestSql( $test );

            // Either show the query or migrate the test or report missing sql file?
            if( $sql ) {
                if( $this->showQuery === true ) {
                    $this->showSqlQuery( $sql );
                } else {
                    $this->migrateTestData( $sql, $test );
                }
            } else {
                $this->console->writeLine(
                    sprintf( "No SQL script found for '%s' in '%s'...", $test->getTestKey(), $this->database
                    )
                );
            }
        }
    }

    /**
     * Get the SQL string from the SQL file by test using mapper
     * @param object $test
     * @return string
     */
    private function getTestSql( $test )
    {
        if( array_key_exists( $test->getTestKey(), $this->testMigrationSql ) ) {
            $sql = $this->testMigrationSql[ $test->getTestKey() ];
        } else {
            $sql = $this->testMigrationSqlMapper->map( $this->sqlScriptsDirectory . $test->getTestKey() );
            $this->testMigrationSql[ $test->getTestKey() ] = $sql;
        }
        return $sql;
    }

    /**
     * @param string $sql
     * @param Test $test
     */
    private function migrateTestData( $sql, $test )
    {
        if( $this->remigrate ) {
            $this->checkForMigratedTestData( $test );
        }
        $rows = $this->queryClientData( $sql );
        $tests = $this->testsRepo->findOneByTestFilename( $test->getTestKey() );

        if( $rows && $this->writeToCsvFile( $rows ) ) {
            // Perform soft-validation check here
            $response = $this->loadCsvS3AndSoftValidate( $test );

            // Import if we pass soft-validation
            if( $response ) {
                $this->queueImportTestJob( $response );
            } else {
                error_log( sprintf( "No soft-validation response for test '%s' on '%s'...", $test->getTestKey(), $this->database ) );
            }
        } elseif( empty( $rows ) ) {
            error_log( sprintf( "No data to migrate for Test %s on %s", $this->test, $this->database ) );
            $this->setTestAsMigrated( $tests );
        }
        if( $tests->isEnabled() && !$test->isEnabled() ) {
            $test->enable();
            $this->saveEntity( $test );
        }
    }

    private function checkForMigratedTestData( $test )
    {
        $testImportRepo = $this->doctrine->getClientEm()->getRepository( 'CompanyTech\DataWarehouse\Entity\TestImport' );
        $migratedTestImports = $testImportRepo->findByTestIdAndPayloadMigration( $test->getId() );

        if( !empty($migratedTestImports) ) {
            foreach($migratedTestImports as $testImport) {
                $this->deletePreviousTestMigratedData($testImport);
            }
        }
    }

    private function setTestAsMigrated( $tests )
    {
        $tests->setTestType( 'migrated' );
        $this->saveEntity( $tests );
    }

    /**
     * Show which table (test) and SQL file was used
     * @param string $sql
     */
    private function showSqlQuery( $sql )
    {
        $from = preg_match( '/FROM\s+(\w+)/', $sql );
        $this->console->writeLine(
            sprintf( "from sql file %s and table :\n%s", $this->test, $from
            )
        );
    }

    /**
     * Write to the new datafile and show rowCount, client database and test to console
     * @param array $rows
     * @return boolean
     */
    private function writeToCsvFile( $rows )
    {
        $this->createDataFile();

        if( empty( $rows ) || (!$this->filehandle) ) {
            return false;
        }
        $this->createHeader( $rows );
        $rowCount = count( $rows );

        foreach( $rows as $row ) {
            fputcsv( $this->filehandle, $row, ',', '"' );
        }
        fclose( $this->filehandle );

        $this->console->writeLine(
            sprintf( "new migrated datafile written out with %d row%s for client %s and test %s", $rowCount, ($rowCount > 1 ? 's' : null ), $this->database, $this->test
            )
        );
        return true;
    }

    /**
     * Create the new datafile
     */
    private function createDataFile()
    {
        $client = ltrim( $this->database, 'app_' );

        $this->builtFilename = $client . '.migration.datafile.' . $this->test . '.' .
            (isset( $this->filename ) ? $this->filename . '.' : '') .
            $this->getDateTimeForFileName() . '.csv';

        $filenamepath = $this->filepath . DIRECTORY_SEPARATOR . $this->builtFilename;

        try {
            $this->filehandle = fopen( $filenamepath, 'w+' );
            $this->filenamepath = $filenamepath;
        } catch( Exception $ex ) {
            throw new \Exception( sprintf( "Error - could not open filename: %s  %s", $filenamepath, $ex->getMessage() ) );
        }
    }

    /**
     * Create the header columns for new datafile
     * @param array $rows
     * @return array
     */
    private function createHeader( $rows )
    {
        $formattedColumns = array_keys( $rows[ 0 ] );
        fputcsv( $this->filehandle, $formattedColumns );
        return $formattedColumns;
    }

    /**
     * Upload new datafile to S3 and soft validate it
     */
    private function loadCsvS3AndSoftValidate( $test )
    {
        $validationResponse = null;
        $validateUploadHandler = $this->xmanager->get( 'datawarehouse.validate_upload.handler' );

        try {
            $s3response = $this->uploadFileToS3();

            // Need to create a new upload file record with UploadedFile entity, set the owner
            $uploadedFileEntity = $this->createUploadedFileEntity( $s3response );

            // Build the TestImport Entity and add to client's core_upload_file
            $testImportEntity = $validateUploadHandler->softValidateAndCreateTestImport( $test, $uploadedFileEntity, true );
            $entityManager = $this->doctrine->getClientEm();
            $entityManager->persist( $testImportEntity );
            $entityManager->flush();

            $validationResponse = $testImportEntity;
        } catch( Exception $ex ) {
            echo sprintf( "Error %s\n", $ex->getMessage()
            );
        }

        return $validationResponse;
    }

    /**
     * instantiate the queue configuration object
     * @param object $testImportEntity
     */
    private function queueImportTestJob( $testImportEntity )
    {
        $queueConfig = $this->setupQueueConfig();

        $payload = array(
            "testImportId" => $testImportEntity->getId(),
            "migration" => true
        );
        $queueConfig
            ->appQueueManager
            ->createQueueJob(
                new TestImportJob(), QueuePayload::payload(
                    AppQueueManager::SYNC, $queueConfig->database, $queueConfig->email, $payload
                )
        );

        $this->console->writeLine(
            sprintf( "Successfully queued import for test '%s' on client '%s'", $this->test, $this->database
            )
        );
    }

    /**
     * Setup the queue configuration based on configured values
     * @return \stdClass
     */
    private function setupQueueConfig()
    {
        $queueConfig = new \stdClass();
        $queueConfig->appQueueManager = $this->xmanager->getServiceLocator()->get( 'queue.manager' );
        $queueConfig->database = $this->database;
        $queueConfig->email = $this->email;

        return $queueConfig;
    }

    /**
     * Upload the new datafile to S3
     * @return object
     * @throws \Exception
     */
    private function uploadFileToS3()
    {
        $this->console->writeLine( "loading new datafile to S3" );

        if( !isset( $this->subscriptionId ) ) {
            throw new \Exception( "subscription id is not set" );
        }
        $this->remoteBucketPath = '/subscription/' . $this->subscriptionId . '/DATA-WAREHOUSE/'
            . $this->builtFilename;
        return $this->s3Uploader->uploadFile( $this->remoteBucketPath, $this->filenamepath );
    }

    /**
     * Create entity and save it for the uploaded file
     * @param object $s3response
     * @return object UploadedFile
     */
    private function createUploadedFileEntity( $s3response )
    {
        $uploadedFile = new UploadedFile();
        $uploadedFile->setName( $this->builtFilename );
        $uploadedFile->setFileKey( $this->remoteBucketPath );
        $uploadedFile->setObjectUrl( $s3response->get( 'ObjectURL' ) );
        $uploadedFile->setModule( 'DATA-WAREHOUSE' );
        $uploadedFile->setFileType( 'text/csv' );
        $uploadedFile->setEtag( $s3response->get( 'ETag' ) );
        $uploadedFile->setSubscription( $this->subscriptionId );
        $uploadedFile->setOwner( $this->getFirstClientUser() );

        $entityManager = $this->doctrine->getClientEm();
        $entityManager->persist( $uploadedFile );
        $entityManager->flush();

        return $uploadedFile;
    }

    /**
     * Get the first user record object from the client database
     * @return object
     */
    private function getFirstClientUser()
    {
        $userRepo = $this->doctrine->getClientEm()->getRepository( 'CompanyTech\Core\Entity\Client\ClientUser' );
        return $userRepo->getFirstClientUser();
    }

    /**
     * @param Entity $entity
     */
    public function saveEntity( $entity )
    {
        $this->doctrine->getClientEm()->persist( $entity );
        $this->doctrine->getClientEm()->flush();
    }

    /**
     * @param object $testImport
     */
    protected function deletePreviousTestMigratedData( $testImport )
    {
        /** @var TestImportRepository $testImportRepository */
        $testImportRepository = $this->doctrine->getClientEm()->getRepository( TestImport::class );
        $testImportRepository->removeTestImportCollectionsDql( $testImport );
        
        $this->doctrine->getClientEm()->remove($testImport);
        
        $this->console->writeLine( "Removed previous migrated test data for TestImport id: " . $testImport->getId() );
    }
}
