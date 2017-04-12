<?php

namespace ScoutEngines\Elasticsearch;

use Laravel\Scout\Builder;
use Elasticsearch\Client as Elasticsearch;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Support\Collection as BaseCollection;
use Laravel\Scout\Engines\Engine;

class ElasticsearchEngine extends Engine
{
    /**
     * The Elasticsearch client instance.
     *
     * @var \Elasticsearch\Client
     */
    protected $elasticsearch;

    /**
     * The index name.
     *
     * @var string
     */
    protected $index;

    /**
     * Soft delete
     *
     * @var bool
     */
    protected $showTrashed = false;

    /**
     * Create a new engine instance.
     *
     * @param  \Elasticsearch\Client  $elasticsearch
     * @return void
     */
    public function __construct(Elasticsearch $elasticsearch, $index)
    {
        $this->elasticsearch = $elasticsearch;

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
        $body = new BaseCollection();

        $models->each(function ($model) use ($body) {
            $array = $model->toSearchableArray();

            if (empty($array)) {
                return;
            }

            $body->push([
                'index' => [
                    '_index' => $this->index,
                    '_type' => $model->searchableAs(),
                    '_id' => $model->getKey(),
                ],
            ]);

            $body->push($array);
        });

        $this->elasticsearch->bulk([
            'refresh' => true,
            'body' => $body->all(),
        ]);
    }

    /**
     * Remove the given model from the index.
     *
     * @param  Collection  $models
     * @return void
     */
    public function delete($models)
    {
        $body = new BaseCollection();

        $models->each(function ($model) use ($body) {
            $body->push([
                'delete' => [
                    '_index' => $this->index,
                    '_type' => $model->searchableAs(),
                    '_id'  => $model->getKey(),
                ],
            ]);
        });

        $this->elasticsearch->bulk([
            'refresh' => true,
            'body' => $body->all(),
        ]);
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
     * @param  Builder  $query
     * @return mixed
     */
    public function search(Builder $query)
    {
        return $this->performSearch($query, [
            'filters' => $this->filters($query),
            'size' => $query->limit ?: 10000,
        ]);
    }

    /**
     * Perform the given search on the engine.
     *
     * @param  Builder  $query
     * @param  int  $perPage
     * @param  int  $page
     * @return mixed
     */
    public function paginate(Builder $query, $perPage, $page)
    {
        $result = $this->performSearch($query, [
            'filters' => $this->filters($query),
            'size' => $perPage,
            'from' => (($page * $perPage) - $perPage),
        ]);

        $result['nbPages'] = (int) ceil($result['hits']['total'] / $perPage);

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
        $filters = [];
        $matches = [];

        if (is_null($builder->query) || empty($builder->query)) {
            $matches[] = [
                'match_all' => []
            ];
        }

        if (is_string($builder->query)) {
            $matches[] = [
                'match' => [
                    '_all' => [
                        'query' => $builder->query,
                        'fuzziness' => 1
                    ]
                ]
            ];
        }

        if (is_array($builder->query) && count($builder->query)) {
            /**
             * Geo distance
             */
            if ($geo = array_get($builder->query, 'geo_distance')) {
                $attribute = array_get($geo, 'attribute', 'location');
                $filters[] = [
                    'geo_distance' => [
                        'distance' => array_get($geo, 'distance', '3km'),
                        "distance_type" => 'plane',
                         $attribute => [
                            'lon' => array_get($geo, 'lng'),
                            'lat' => array_get($geo, 'lat')
                         ]
                    ]
                ];
            }

            if ($skip = array_get($builder->query, 'skip')) {
                $query['from'] = $skip;
            }

            if ($limit = array_get($builder->query, 'limit')) {
                $query['size'] = $limit;
            }
        }

        if (array_key_exists('filters', $options) && $options['filters']) {
            foreach ($options['filters'] as $field => $value) {

                if (is_numeric($value)) {
                    $filters[] = [
                        'term' => [
                            $field => $value,
                        ],
                    ];
                } elseif (is_string($value)) {
                    $matches[] = [
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

        $query = [
            'index' =>  $this->index,
            'type'  =>  $builder->model->searchableAs(),
            'body' => [
                'query' => [
                    'filtered' => [
                        'filter' => $filters,
                        'query' => [
                            'bool' => [
                                'must' => $matches
                            ]
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

        if (array_key_exists('size', $options)) {
            $query['size'] = $options['size'];
        }

        if (array_key_exists('from', $options)) {
            $query['from'] = $options['from'];
        }

        if ($builder->callback) {
            return call_user_func(
                $builder->callback,
                $this->elasticsearch,
                $query
            );
        }

        return $this->elasticsearch->search($query);
    }

    /**
     * Get the filter array for the query.
     *
     * @param  Builder  $query
     * @return array
     */
    protected function filters(Builder $query)
    {
        return $query->wheres;
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
     * @param  mixed  $results
     * @param  \Illuminate\Database\Eloquent\Model  $model
     * @return Collection
     */
    public function map($results, $model)
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
}

