db.movies_metadata.aggregate([
    // TODO: Write your query here
    {
        $project: {
            tag: {
                $split: [
                { $toLower: '$tagline' }
                , " "]
            }
        }
    },
    { $unwind: "$tag" },
    {
        $project: {
            tag: { $trim: { input: "$tag", chars: ".!?," } },
            length: { $strLenCP:  { $trim: { input: "$tag", chars: ".!?," }}}
        }
    },
     { $match: { length: { $gt: 3 } } },
     {
        $group: {
            _id: "$tag",
            count: {$sum: 1}
        }
    },
    {$sort: {count: -1}},
    {$limit: 20}
]);