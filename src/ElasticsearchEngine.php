<?php

namespace ScoutEngines\Elasticsearch;

use Illuminate\Support\Arr;
use Laravel\Scout\Builder;
use Laravel\Scout\Engines\Engine;
use Elasticsearch\Client as Elastic;
use Illuminate\Database\Eloquent\Collection;

class ElasticsearchEngine extends Engine
{
    /**
     * Index where the models will be saved.
     *
     * @var string
     */
    protected $index;
    
    /**
     * Elastic where the instance of Elastic|\Elasticsearch\Client is stored.
     *
     * @var object
     */
    protected $elastic;

    /**
     * Soft delete
     *
     * @var bool
     */
    protected $showTrashed = false;

    /**
     * Create a new engine instance.
     *
     * @param  \Elasticsearch\Client  $elastic
     * @return void
     */
    public function __construct(Elastic $elastic, $index)
    {
        $this->elastic = $elastic;
        $this->index = $index;
    }

    /**
     * Update the given model in the index.
     *
     * @param  Collection  $models
     * @return void
     */
    public function update($models)
    {
        $params['refresh'] = true;
        $params['body'] = [];

        $models->each(function($model) use (&$params)
        {
            $doc = $model->toSearchableArray();

            if (empty($doc)) {
                return true;
            }

            $params['body'][] = [
                'update' => [
                    '_id' => $model->getKey(),
                    '_index' => $this->index,
                    '_type' => $model->searchableAs(),
                ]
            ];
            $params['body'][] = [
                'doc' => $doc,
                'doc_as_upsert' => true
            ];
        });

        if (!empty($params['body'])) {
            $this->elastic->bulk($params);
        }
    }

    /**
     * Remove the given model from the index.
     *
     * @param  Collection  $models
     * @return void
     */
    public function delete($models)
    {
        $params['refresh'] = true;
        $params['body'] = [];

        $models->each(function($model) use (&$params)
        {
            $params['body'][] = [
                'delete' => [
                    '_id' => $model->getKey(),
                    '_index' => $this->index,
                    '_type' => $model->searchableAs(),
                ]
            ];
        });

        $this->elastic->bulk($params);
    }

    /**
     * Return deleted documents
     *
     * @param bool $value
     * @return $this
     */
    public function withTrashed($value = true)
    {
        $this->showTrashed = $value;
        return $this;
    }

    /**
     * Perform the given search on the engine.
     *
     * @param  Builder  $builder
     * @return mixed
     */
    public function search(Builder $builder)
    {
        return $this->performSearch($builder, array_filter([
            'filters' => $this->filters($builder),
            'rawFilters' => $this->rawFilters($builder),
            'size' => $builder->limit ?: 10000,
        ]));
    }

    /**
     * Perform the given search on the engine.
     *
     * @param  Builder  $builder
     * @param  int  $perPage
     * @param  int  $page
     * @return mixed
     */
    public function paginate(Builder $builder, $perPage, $page)
    {
        $result = $this->performSearch($builder, [
            'filters' => $this->filters($builder),
            'rawFilters' => $this->rawFilters($builder),
            'from' => (($page * $perPage) - $perPage),
            'size' => $perPage,
        ]);

       $result['nbPages'] = (int) ceil($result['hits']['total'] / $perPage);;

        return $result;
    }

    /**
     * Perform the given search on the engine.
     *
     * @param  Builder  $builder
     * @param  array  $options
     * @return mixed
     */
    protected function performSearch(Builder $builder, array $options = [])
    {
        $raw        = Arr::get($options, 'rawFilters', []);
        $must       = [];
        $should     = [];
        $filters    = [];
        
        if (is_null($builder->query) || empty($builder->query)) {
            $must[] = [
                'match_all' => []
            ];
        } elseif (is_string($builder->query)) {
            $must[] = [
                'match' => [
                    '_all' => [
                        'query' => $builder->query,
                        'fuzziness' => 1
                    ]
                ]
            ];
        } elseif (is_array($builder->query)) {
            /**
             * Geo distance
             */
            if ($geo = Arr::get($builder->query, 'geo_distance')) {
                $attribute = Arr::get($geo, 'attribute', 'location');
                $filters[] = [
                    'geo_distance' => [
                        'distance' => Arr::get($geo, 'distance', '3km'),
                        "distance_type" => 'plane',
                        $attribute => [
                            'lon' => Arr::get($geo, 'lng'),
                            'lat' => Arr::get($geo, 'lat')
                        ]
                    ]
                ];
            }

            if ($skip = Arr::get($builder->query, 'skip')) {
                $query['from'] = $skip;
            }

            if ($limit = Arr::get($builder->query, 'limit')) {
                $query['size'] = $limit;
            }

            if ($on = Arr::get($builder->query, 'on')) {
                $must[] = [
                    'match' => $on,
                ];
            }
        }

        if (Arr::get($options, 'filters')) {
            foreach ($options['filters'] as $field => $value) {
                if (is_numeric($value)) {
                    $must[] = [
                        'term' => [
                            $field => $value,
                        ],
                    ];
                } elseif (is_string($value)) {
                    $must[] = [
                        'match' => [
                            $field => [
                                'query' => $value,
                                'operator' => 'and'
                            ]
                        ]
                    ];
                }
            }
        }

        $queryData = [
            'must' => $must,
            'should' => $should,
            'must_not' => []
        ];

        foreach ($raw as $r) {
            $queryData = array_merge($queryData, $r);
        }

        $query = [
            'index' =>  $this->index,
            'type'  =>  $builder->model->searchableAs(),
            'body' => [
                'query' => [
                    'filtered' => [
                        'filter' => $filters,
                        'query' => [
                            'bool' => $queryData
                        ],
                    ],
                ],
            ],
        ];

        if ($builder->orders) {
            $sort = [];
            foreach ($builder->orders as $order) {
                $attribute = $order['column'];
                $sort[] = [
                    $attribute => [
                        'order' => $order['direction']
                    ]
                ];
            }
            $query['body']['sort'] = $sort;
        }

        if (Arr::has($options, 'size')) {
            $query['size'] = $options['size'];
        }

        if (Arr::has($options, 'from')) {
            $query['from'] = $options['from'];
        }

        if ($builder->callback) {
            return call_user_func(
                $builder->callback,
                $this->elastic,
                $query
            );
        }

        return $this->elastic->search($query);
    }

    /**
     * Get the filter array for the query.
     *
     * @param  Builder  $builder
     * @return array
     */
    protected function filters(Builder $builder)
    {
        return $builder->wheres;
    }

    /**
     * Get the raw filter array for the query.
     *
     * @param  Builder  $builder
     * @return array
     */
    protected function rawFilters(Builder $builder)
    {
        return $builder->filters;
    }

    /**
     * Pluck and return the primary keys of the given results.
     *
     * @param  mixed  $results
     * @return \Illuminate\Support\Collection
     */
    public function mapIds($results)
    {
        return collect($results['hits']['hits'])->pluck('_id')->values();
    }

    /**
     * Map the given results to instances of the given model.
     *
     * @param  \Laravel\Scout\Builder  $builder
     * @param  mixed  $results
     * @param  \Illuminate\Database\Eloquent\Model  $model
     * @return Collection
     */
    public function map(Builder $builder, $results, $model)
    {
        if (count($results['hits']) === 0) {
            return Collection::make();
        }

        $keys = collect($results['hits']['hits'])
            ->pluck('_id')
            ->values()
            ->all();

        $modelQuery = $model->whereIn(
            $model->getQualifiedKeyName(),
            $keys
        );

        if ($this->showTrashed) {
            $modelQuery->withTrashed();
        }

        $models = $modelQuery->get()->keyBy($model->getKeyName());

        return Collection::make($results['hits']['hits'])->map(function ($hit) use ($model, $models) {
            return isset($models[$hit['_source'][$model->getKeyName()]])
                ? $models[$hit['_source'][$model->getKeyName()]] : null;
        })->filter()->values();
    }
    /**
     * Get the total count from a raw result returned by the engine.
     *
     * @param  mixed  $results
     * @return int
     */
    public function getTotalCount($results)
    {
        return $results['hits']['total'];
    }

    /**
     * Flush all of the model's records from the engine.
     *
     * @param  \Illuminate\Database\Eloquent\Model  $model
     * @return void
     */
    public function flush($model)
    {
        $model->newQuery()
            ->orderBy($model->getKeyName())
            ->unsearchable();
    }

    /**
     * Generates the sort if theres any.
     *
     * @param  Builder $builder
     * @return array|null
     */
    protected function sort($builder)
    {
        if (count($builder->orders) == 0) {
            return null;
        }

        return collect($builder->orders)->map(function($order) {
            return [$order['column'] => $order['direction']];
        })->toArray();
    }
}
