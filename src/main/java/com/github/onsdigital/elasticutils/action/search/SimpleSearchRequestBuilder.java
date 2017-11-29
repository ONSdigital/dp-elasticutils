package com.github.onsdigital.elasticutils.action.search;

import com.github.onsdigital.elasticutils.action.SimpleActionRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;

import java.util.Arrays;
import java.util.List;

/**
 * @author sullid (David Sullivan) on 29/11/2017
 * @project dp-elasticutils
 */
public class SimpleSearchRequestBuilder implements SimpleActionRequest<SearchRequest> {

    private SearchRequest request;

    public SimpleSearchRequestBuilder(String... indices) {
        request = new SearchRequest().indices(indices);
    }

    @Override
    public SearchRequest request() {
        return this.request;
    }

    /**
     * Sets the indices the search will be executed on.
     */
    public SimpleSearchRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * The document types to execute the search against. Defaults to be executed against
     * all types.
     */
    public SimpleSearchRequestBuilder setTypes(String... types) {
        request.types(types);
        return this;
    }

    /**
     * The search type to execute, defaults to {@link org.elasticsearch.action.search.SearchType#DEFAULT}.
     */
    public SimpleSearchRequestBuilder setSearchType(SearchType searchType) {
        request.searchType(searchType);
        return this;
    }

    /**
     * The a string representation search type to execute, defaults to {@link SearchType#DEFAULT}. Can be
     * one of "dfs_query_then_fetch"/"dfsQueryThenFetch", "dfs_query_and_fetch"/"dfsQueryAndFetch",
     * "query_then_fetch"/"queryThenFetch", and "query_and_fetch"/"queryAndFetch".
     */
    public SimpleSearchRequestBuilder setSearchType(String searchType) {
        request.searchType(searchType);
        return this;
    }

    /**
     * If set, will enable scrolling of the search request.
     */
    public SimpleSearchRequestBuilder setScroll(Scroll scroll) {
        request.scroll(scroll);
        return this;
    }

    /**
     * If set, will enable scrolling of the search request for the specified timeout.
     */
    public SimpleSearchRequestBuilder setScroll(TimeValue keepAlive) {
        request.scroll(keepAlive);
        return this;
    }

    /**
     * If set, will enable scrolling of the search request for the specified timeout.
     */
    public SimpleSearchRequestBuilder setScroll(String keepAlive) {
        request.scroll(keepAlive);
        return this;
    }

    /**
     * An optional timeout to control how long search is allowed to take.
     */
    public SimpleSearchRequestBuilder setTimeout(TimeValue timeout) {
        sourceBuilder().timeout(timeout);
        return this;
    }

    /**
     * An optional document count, upon collecting which the search
     * query will early terminate
     */
    public SimpleSearchRequestBuilder setTerminateAfter(int terminateAfter) {
        sourceBuilder().terminateAfter(terminateAfter);
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public SimpleSearchRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    /**
     * The routing values to control the shards that the search will be executed on.
     */
    public SimpleSearchRequestBuilder setRouting(String... routing) {
        request.routing(routing);
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * <tt>_local</tt> to prefer local shards or a custom value, which guarantees that the same order
     * will be used across different requests.
     */
    public SimpleSearchRequestBuilder setPreference(String preference) {
        request.preference(preference);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions.
     * <p>
     * For example indices that don't exist.
     */
    public SimpleSearchRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request().indicesOptions(indicesOptions);
        return this;
    }

    /**
     * Constructs a new search source builder with a search query.
     *
     * @see org.elasticsearch.index.query.QueryBuilders
     */
    public SimpleSearchRequestBuilder setQuery(QueryBuilder queryBuilder) {
        sourceBuilder().query(queryBuilder);
        return this;
    }

    /**
     * Sets a filter that will be executed after the query has been executed and only has affect on the search hits
     * (not aggregations). This filter is always executed as last filtering mechanism.
     */
    public SimpleSearchRequestBuilder setPostFilter(QueryBuilder postFilter) {
        sourceBuilder().postFilter(postFilter);
        return this;
    }

    /**
     * Sets the minimum score below which docs will be filtered out.
     */
    public SimpleSearchRequestBuilder setMinScore(float minScore) {
        sourceBuilder().minScore(minScore);
        return this;
    }

    /**
     * From index to start the search from. Defaults to <tt>0</tt>.
     */
    public SimpleSearchRequestBuilder setFrom(int from) {
        sourceBuilder().from(from);
        return this;
    }

    /**
     * The number of search hits to return. Defaults to <tt>10</tt>.
     */
    public SimpleSearchRequestBuilder setSize(int size) {
        sourceBuilder().size(size);
        return this;
    }

    /**
     * Should each {@link org.elasticsearch.search.SearchHit} be returned with an
     * explanation of the hit (ranking).
     */
    public SimpleSearchRequestBuilder setExplain(boolean explain) {
        sourceBuilder().explain(explain);
        return this;
    }

    /**
     * Should each {@link org.elasticsearch.search.SearchHit} be returned with its
     * version.
     */
    public SimpleSearchRequestBuilder setVersion(boolean version) {
        sourceBuilder().version(version);
        return this;
    }

    /**
     * Sets the boost a specific index will receive when the query is executed against it.
     *
     * @param index      The index to apply the boost against
     * @param indexBoost The boost to apply to the index
     */
    public SimpleSearchRequestBuilder addIndexBoost(String index, float indexBoost) {
        sourceBuilder().indexBoost(index, indexBoost);
        return this;
    }

    /**
     * The stats groups this request will be aggregated under.
     */
    public SimpleSearchRequestBuilder setStats(String... statsGroups) {
        sourceBuilder().stats(Arrays.asList(statsGroups));
        return this;
    }

    /**
     * The stats groups this request will be aggregated under.
     */
    public SimpleSearchRequestBuilder setStats(List<String> statsGroups) {
        sourceBuilder().stats(statsGroups);
        return this;
    }

    /**
     * Indicates whether the response should contain the stored _source for every hit
     */
    public SimpleSearchRequestBuilder setFetchSource(boolean fetch) {
        sourceBuilder().fetchSource(fetch);
        return this;
    }

    /**
     * Indicate that _source should be returned with every hit, with an "include" and/or "exclude" set which can include simple wildcard
     * elements.
     *
     * @param include An optional include (optionally wildcarded) pattern to filter the returned _source
     * @param exclude An optional exclude (optionally wildcarded) pattern to filter the returned _source
     */
    public SimpleSearchRequestBuilder setFetchSource(@Nullable String include, @Nullable String exclude) {
        sourceBuilder().fetchSource(include, exclude);
        return this;
    }

    /**
     * Indicate that _source should be returned with every hit, with an "include" and/or "exclude" set which can include simple wildcard
     * elements.
     *
     * @param includes An optional list of include (optionally wildcarded) pattern to filter the returned _source
     * @param excludes An optional list of exclude (optionally wildcarded) pattern to filter the returned _source
     */
    public SimpleSearchRequestBuilder setFetchSource(@Nullable String[] includes, @Nullable String[] excludes) {
        sourceBuilder().fetchSource(includes, excludes);
        return this;
    }

    /**
     * Adds a docvalue based field to load and return. The field does not have to be stored,
     * but its recommended to use non analyzed or numeric fields.
     *
     * @param name The field to get from the docvalue
     */
    public SimpleSearchRequestBuilder addDocValueField(String name) {
        sourceBuilder().docValueField(name);
        return this;
    }

    /**
     * Adds a stored field to load and return (note, it must be stored) as part of the search request.
     */
    public SimpleSearchRequestBuilder addStoredField(String field) {
        sourceBuilder().storedField(field);
        return this;
    }

    /**
     * Adds a script based field to load and return. The field does not have to be stored,
     * but its recommended to use non analyzed or numeric fields.
     *
     * @param name   The name that will represent this value in the return hit
     * @param script The script to use
     */
    public SimpleSearchRequestBuilder addScriptField(String name, Script script) {
        sourceBuilder().scriptField(name, script);
        return this;
    }

    /**
     * Adds a sort against the given field name and the sort ordering.
     *
     * @param field The name of the field
     * @param order The sort ordering
     */
    public SimpleSearchRequestBuilder addSort(String field, SortOrder order) {
        sourceBuilder().sort(field, order);
        return this;
    }

    /**
     * Adds a generic sort builder.
     *
     * @see org.elasticsearch.search.sort.SortBuilders
     */
    public SimpleSearchRequestBuilder addSort(SortBuilder sort) {
        sourceBuilder().sort(sort);
        return this;
    }

    /**
     * Set the sort values that indicates which docs this request should "search after".
     *
     */
    public SimpleSearchRequestBuilder searchAfter(Object[] values) {
        sourceBuilder().searchAfter(values);
        return this;
    }

    public SimpleSearchRequestBuilder slice(SliceBuilder builder) {
        sourceBuilder().slice(builder);
        return this;
    }

    /**
     * Applies when sorting, and controls if scores will be tracked as well. Defaults to <tt>false</tt>.
     */
    public SimpleSearchRequestBuilder setTrackScores(boolean trackScores) {
        sourceBuilder().trackScores(trackScores);
        return this;
    }

    /**
     * Indicates if the total hit count for the query should be tracked. Defaults to <tt>true</tt>
     */
    public SimpleSearchRequestBuilder setTrackTotalHits(boolean trackTotalHits) {
        sourceBuilder().trackTotalHits(trackTotalHits);
        return this;
    }

    /**
     * Adds stored fields to load and return (note, it must be stored) as part of the search request.
     * To disable the stored fields entirely (source and metadata fields) use {@code storedField("_none_")}.
     * @deprecated Use {@link SimpleSearchRequestBuilder#storedFields(String...)} instead.
     */
    @Deprecated
    public SimpleSearchRequestBuilder fields(String... fields) {
        sourceBuilder().storedFields(Arrays.asList(fields));
        return this;
    }

    /**
     * Adds stored fields to load and return (note, it must be stored) as part of the search request.
     * To disable the stored fields entirely (source and metadata fields) use {@code storedField("_none_")}.
     */
    public SimpleSearchRequestBuilder storedFields(String... fields) {
        sourceBuilder().storedFields(Arrays.asList(fields));
        return this;
    }

    /**
     * Adds an aggregation to the search operation.
     */
    public SimpleSearchRequestBuilder addAggregation(AggregationBuilder aggregation) {
        sourceBuilder().aggregation(aggregation);
        return this;
    }

    /**
     * Adds an aggregation to the search operation.
     */
    public SimpleSearchRequestBuilder addAggregation(PipelineAggregationBuilder aggregation) {
        sourceBuilder().aggregation(aggregation);
        return this;
    }

    public SimpleSearchRequestBuilder highlighter(HighlightBuilder highlightBuilder) {
        sourceBuilder().highlighter(highlightBuilder);
        return this;
    }

    /**
     * Delegates to {@link SearchSourceBuilder#suggest(SuggestBuilder)}
     */
    public SimpleSearchRequestBuilder suggest(SuggestBuilder suggestBuilder) {
        sourceBuilder().suggest(suggestBuilder);
        return this;
    }

    /**
     * Clears all rescorers from the builder.
     *
     * @return this for chaining
     */
    public SimpleSearchRequestBuilder clearRescorers() {
        sourceBuilder().clearRescorers();
        return this;
    }

    /**
     * Sets the source of the request as a SearchSourceBuilder.
     */
    public SimpleSearchRequestBuilder setSource(SearchSourceBuilder source) {
        request.source(source);
        return this;
    }

    /**
     * Sets if this request should use the request cache or not, assuming that it can (for
     * example, if "now" is used, it will never be cached). By default (not set, or null,
     * will default to the index level setting if request cache is enabled or not).
     */
    public SimpleSearchRequestBuilder setRequestCache(Boolean requestCache) {
        request.requestCache(requestCache);
        return this;
    }

    /**
     * Should the query be profiled. Defaults to <code>false</code>
     */
    public SimpleSearchRequestBuilder setProfile(boolean profile) {
        sourceBuilder().profile(profile);
        return this;
    }

    public SimpleSearchRequestBuilder setCollapse(CollapseBuilder collapse) {
        sourceBuilder().collapse(collapse);
        return this;
    }

    @Override
    public String toString() {
        if (request.source() != null) {
            return request.source().toString();
        }
        return new SearchSourceBuilder().toString();
    }

    private SearchSourceBuilder sourceBuilder() {
        if (request.source() == null) {
            request.source(new SearchSourceBuilder());
        }
        return request.source();
    }

    /**
     * Sets the number of shard results that should be reduced at once on the coordinating node. This value should be used as a protection
     * mechanism to reduce the memory overhead per search request if the potential number of shards in the request can be large.
     */
    public SimpleSearchRequestBuilder setBatchedReduceSize(int batchedReduceSize) {
        this.request.setBatchedReduceSize(batchedReduceSize);
        return this;
    }

    /**
     * Sets the number of shard requests that should be executed concurrently. This value should be used as a protection mechanism to
     * reduce the number of shard requests fired per high level search request. Searches that hit the entire cluster can be throttled
     * with this number to reduce the cluster load. The default grows with the number of nodes in the cluster but is at most <tt>256</tt>.
     */
    public SimpleSearchRequestBuilder setMaxConcurrentShardRequests(int maxConcurrentShardRequests) {
        this.request.setMaxConcurrentShardRequests(maxConcurrentShardRequests);
        return this;
    }

    /**
     * Sets a threshold that enforces a pre-filter roundtrip to pre-filter search shards based on query rewriting if the number of shards
     * the search request expands to exceeds the threshold. This filter roundtrip can limit the number of shards significantly if for
     * instance a shard can not match any documents based on it's rewrite method ie. if date filters are mandatory to match but the shard
     * bounds and the query are disjoint. The default is <tt>128</tt>
     */
    public SimpleSearchRequestBuilder setPreFilterShardSize(int preFilterShardSize) {
        this.request.setPreFilterShardSize(preFilterShardSize);
        return this;
    }

}
