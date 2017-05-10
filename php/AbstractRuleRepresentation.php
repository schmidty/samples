<?php

namespace CompanyTech\DataWarehouse\Representations\Rule;

use CompanyTech\DataWarehouse\Exceptions\Validation\InvalidDataColumnException;
use CompanyTech\DataWarehouse\Exceptions\Definition\InvalidRuleDefinitionException;
use CompanyTech\DataWarehouse\Exceptions\Definition\UndefinedCompareMethodException;
use CompanyTech\DataWarehouse\Fabrics\TypeFabric;
use CompanyTech\DataWarehouse\Interfaces\Compare\Comparable;
use CompanyTech\DataWarehouse\Interfaces\Definition\SoftValidationTags;
use CompanyTech\DataWarehouse\Interfaces\Type\Typeable;

/**
 * Class AbstractRuleRepresentation
 * @package CompanyTech\DataWarehouse\Representations\Rule
 */
abstract class AbstractRuleRepresentation
{

    /**
     * @var array
     */
    protected $data;

    /**
     * Boolean value for file data type
     * @var boolean
     */
    protected $dataType;

    /**
     * Array of non-matching cases
     * @var array
     */
    protected static $caseErrorStack;



    /**
     * @param array $data
     * @param boolean $dataType
     */
    public function __construct( $data, $dataType )
    {
        $this->data = $data;
        $this->dataType = $dataType;

        self::$caseErrorStack = [];
    }

    /**
     * @param $definition
     *
     * @return Typeable
     *
     * @throws InvalidRuleDefinitionException
     */
    protected function getVariableType( $definition )
    {
	    $value = $type = null;

        if( is_array( $definition ) ) {

            $type = TypeFabric::getDefaultVariableType();
            if( array_key_exists( SoftValidationTags::TYPE, $definition ) ) {
                $type = $definition[ SoftValidationTags::TYPE ];
            }

            if (isset($this->dataType) && array_key_exists(SoftValidationTags::FW_COLUMNS, $definition)) {
                $columns = $definition[SoftValidationTags::FW_COLUMNS];
                $value = $this->getFwColumnValues($columns);
            } elseif (array_key_exists(SoftValidationTags::COLUMN, $definition)) {
                $value = $this->getColumnValue($definition);
            } elseif (array_key_exists(SoftValidationTags::STRICT, $definition)) {
                $value = $definition[SoftValidationTags::STRICT];
            } elseif (array_key_exists(SoftValidationTags::PATTERN, $definition)) {
                $value = $definition[SoftValidationTags::PATTERN];
            }
        } else {
            $value = $this->getPlainValue( $definition );
            $type = gettype( $value );
        }

        return TypeFabric::getVariableType( $type, $value );
    }

    /**
     * @param array $definition
     *
     * @return array
     *
     * @throws InvalidRuleDefinitionException
     */
    protected function getResult( $definition )
    {
        if( array_key_exists( SoftValidationTags::RULES, $definition ) ) {
            return $definition[ SoftValidationTags::RULES ];
        } elseif( array_key_exists( SoftValidationTags::RESULT, $definition ) ) {
            return $definition[ SoftValidationTags::RESULT ];
        }

        throw new InvalidRuleDefinitionException( sprintf(
            'Rule endpoint should be %s or %s. None of them defined', SoftValidationTags::RESULT, SoftValidationTags::RULES
        ) );
    }

    /**
     * @param Typeable|Comparable $type
     * @param array        $definition
     *
     * @return mixed
     * @throws UndefinedCompareMethodException
     */
    protected function getCompareMethod( $type, $definition )
    {
        $compare = $type->getDefaultComparison();
        if( array_key_exists( SoftValidationTags::COMPARE, $definition ) ) {
            $compare = $definition[ SoftValidationTags::COMPARE ];
        }

        if( !method_exists( $type, $compare ) ) {
            throw new UndefinedCompareMethodException( sprintf(
                'Comparison rule "%s" is undefined for type "%s"', $compare, $type->getType()
            ) );
        }

        return $compare;
    }

    /**
     * @param $columns
     * @return string
     * @throws InvalidDataColumnException
     */
    protected function getFwColumnValues( $columns )
    {
        if (!is_string($this->data)) {
            throw new InvalidDataColumnException("fixedwidth row is not a string");
        }
        if (!isset($columns[0]) && !isset($columns[1])) {
            throw new InvalidDataColumnException("fixedwidth columns not an array");
        }

        $start = $columns[0];
        $end = $columns[1] - $columns[0];
        $data = substr($this->data, $start, $end);
        //error_log("start :{$start}:, end :{$end}:, data ". print_r($data,1));

        return $data;
    }

    /**
     * @param $definition
     * @return mixed
     * @throws InvalidDataColumnException
     */
    protected function getColumnValue( $definition )
    {
        $column = $definition[ SoftValidationTags::COLUMN ];
        if( $column === 'last' ) {
            $column = count( $this->data );

            // fix for empty column and rows at end of file
            while( '' === trim( $this->data[ $column ] ) && $column >= 0 ) {
                $column--;
            }
        }

        if( !isset( $this->data[ $column ] ) ) {
            throw new InvalidDataColumnException( sprintf("missing column %s", $column) );
        }

        return $this->data[ $column ];
    }

    /**
     * @param array $definition
     *
     * @return mixed
     */
    protected function getPlainValue( $definition )
    {
        $value = $definition;
        if( $value === 'columns_count' ) {
            $value = count( $this->data );

            // fix for empty column and rows at end of file
            while( '' === trim( $this->data[ $value ] ) && $value >= 0 ) {
                $value--;
            }
            //error_log(__LINE__ ."  -  value :{$value}: ");
        }

        return $value;
    }

    /**
     * Return the error stack for non-matching cases
     * @return array caseErrorStack
     */
    public static function getCaseErrorStack()
    {
        return self::$caseErrorStack;
    }

}
