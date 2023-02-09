db.keywords.aggregate([
  // Use elemMatch in the $match stage to find movies  with English
  // as a spoken language
  
   {
        $match:
            {
                $or: [
                { 
                    keywords: {
                     $elemMatch: { 
                        name: "mickey mouse" 
                    } 
                } 
            },
            { 
                keywords: {
                 $elemMatch: {
                  name: "marvel comic" 
                        }
                    }

                }
             ]
        }
    },
    { 
        $project: { 
            _id: 0,
            'movieId': '$movieId'
         }
        },
    { 
        $sort: { 
            movieId: 1 
        }
    }
  
]);