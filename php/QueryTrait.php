<?php
/*
 * Copyright © Company
 *
 * For a full copyright notice, see the LICENSE file.
 */

namespace CompanyTech\Core\Traits;

use Doctrine\ORM\EntityManager;

trait QueryTrait
{

    /**
     * @var \Doctrine\DBAL\Connection $connection
     */
    protected $connection;

    /**
     * @var string $tableName
     */
    protected $tableName;

    /**
     * @return string
     */
    abstract protected function getEntityName();

    /**
     * @return EntityManager
     */
    abstract protected function getEntityManager();

    /**
     * INSERT values for respective table
     * NOTE: DQL cannot be used with an INSERT statement
     * @param object|array $dataObject
     * @return string
     */
    public function insert( $dataObject )
    {
        if( is_object( $dataObject ) ) {
            $dataObject = get_object_vars( $dataObject );
        }
        $keys = array_keys( $dataObject );
        $db = $this->getEntityManager()->getConnection();

        $sql = "INSERT INTO {$this->getTableName()} ({$this->getInsertDataStrings( $dataObject )[ 'keys' ]}) VALUES (:" . implode( ', :', $keys ) . ")";
        $stmt = $db->prepare( $sql );
        $stmt->execute( $dataObject );

        return $db->lastInsertId();
    }

    /**
     * @param array $dataArray
     * @param boolean $orOperator
     * @return mixed
     */
    public function getResultArray( $dataArray, $orOperator = false )
    {
        $where = '';

        $unsetValues = [ ];

        if( count( $dataArray ) ) {
            $where = 'WHERE ';
            $operator = ($orOperator ? ' OR ' : ' AND ');

            $i = 0;
            foreach( $dataArray as $key => $value ) {
                if( $value === null ) {
                    $where .= "{$key} IS NULL";
                    $unsetValues[] = $key;
                } else {
                    $where .= "{$key} = ?";
                }

                $where .= ($i < count( $dataArray ) - 1) ? $operator : '';
                $i++;
            }
        }

        foreach( $unsetValues as $u ) {
            unset( $dataArray[ $u ] );
        }

        $sql = "SELECT * FROM {$this->getTableName()} {$where}";
        $statement = $this->getEntityManager()->getConnection()->prepare( $sql );
        $statement->execute( array_values( $dataArray ) );

        return $statement->fetch();
    }

    /**
     * @param array $dataArray
     * @return array
     */
    public function getInsertDataStrings( $dataArray )
    {
        $resultArray[ 'keys' ] = '';
        $resultArray[ 'values' ] = '';

        $i = 0;
        foreach( $dataArray as $key => $value ) {
            $resultArray[ 'keys' ] .= ($i < count( $dataArray ) - 1) ? "`{$key}`, " : "`{$key}`";

            switch( gettype( $value ) ) {
                case 'string':
                    $value = "'{$value}'";
                    break;
                case 'integer':
                    $value = "{$value}";
                    break;
                case 'NULL':
                    $value = "NULL";
                    break;
            }

            $resultArray[ 'values' ] .= ($i < count( $dataArray ) - 1) ? "{$value}, " : "{$value}";
            $i++;
        }

        return $resultArray;
    }

    /**
     * @return string
     */
    protected function getTableName()
    {
        return $this->getEntityManager()->getClassMetadata( $this->getEntityName() )->getTableName();
    }

}
