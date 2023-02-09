// Task 1ii

db.movies_metadata.aggregate([
    // TODO: Write your query here
    {
        $match: {
            vote_count: {
                $gte: 50
            }
        }
    },
    {
        $match: {
            genres: {
                $elemMatch: {
                    name: "Comedy"
                }
            }
        }
    },
    // project the result
    {
        $project: {
            _id: 0,
            title: 1,
            vote_average: 1,
            vote_count: 1, 
            movieId: 1
        }
    },
    // order by vote_average desc, vote_count desc, movieId asc
    {
        $sort: {
            vote_average: -1, 
            vote_count: -1,
            movieId: 1
        }
    },
    // top 50
    {
        $limit: 50
    }
    // select
    
]);