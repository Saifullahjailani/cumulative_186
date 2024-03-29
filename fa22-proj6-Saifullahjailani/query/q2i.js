// Task 2i

db.movies_metadata.aggregate([
    // TODO: Write your query here
    // apply the formula
    {
        $project:
        {
            _id: 0,
            title: 1,
            vote_count: 1,
            score: { $round: [{ $add: [
                        { $divide: [{ $multiply: ["$vote_count", "$vote_average"] }, { $add: ["$vote_count", 1838] }] },
                        { $divide: [1838*7, { $add: ["$vote_count", 1838] }] }]
                }, 2]},
        }
    },
    { $sort: { score: -1, vote_count: -1, title: 1 } },
    { $limit: 20}
])