
use bc2402_gp

db.mcdonaldata.find({
    health_flag: "⚠ High Risk"
})


// Q10

// creates new variable categorising High-Risk items
db.mcdonaldata.updateMany(
    { $or: [ 
        {totalfat:{$gte:30}}, 
        {sodium:{$gte:1000}}, 
        {cholestrol:{$gte:30}}
        ] },
    { $set: {health_flag: "⚠ High Risk"}}
)

// adds to "health_flag" categorising Moderate-Risk items
db.mcdonaldata.updateMany(
    { $nor: [ 
        {totalfat:{$gte:30}}, 
        {sodium:{$gte:1000}}, 
        {cholestrol:{$gte:30}}
        ] },
    { $set: {health_flag: "✓ Moderate"}}
)

// filters variables wanted
db.mcdonaldata.find(
  {}, {_id: 0, item: 1, calories: 1, totalfat: 1, cholestrol: 1, sodium: 1, health_flag: 1}
)

// ----------------------------------------------------------------
// Q14

db.starbucks.find({})

db.starbucks.aggregate([
    // group documents by "type" and calculate the average calories per type
  { $group: {
      _id: "$type",    // group key = type
      avg_type_calories: {$avg: "$calories"}}   // avg calories for each group
  },
  // clean up the grouped results
  { $project: {
      _id: 0, type: "$_id",   // rename "_id" back to "type"
      avg_type_calories: {$toInt: { $round: ["$avg_type_calories", 0]}}}  // round and convert avg_type_calories to integer
  },
  // lookup back into collection to reattach each item that belongs to each type
  { $lookup: {
      from: "starbucks",
      localField: "type",    // current stage field
      foreignField: "type",    // field in target collection
      as: "items"}   // matched results go into this array
  },
  // unwind the "items" array: creates one document per item, duplicating the avg info for each
  {$unwind: "$items"},
  // final projection and computation delta_from_avg = calories - avg_type_calories (as integer)
  {$project: {
      item: "$items.item", 
      type: 1, 
      calories: "$items.calories", 
      avg_type_calories: 1,
      delta_from_avg: {$toInt: { $round: [{ $subtract: ["$items.calories", "$avg_type_calories"] }, 0]}},
      _id: 0 }
  }
])

// ----------------------------------------------------------------
// Q 19
db.daily_intake.find({})


db.daily_intake.aggregate([
  // Calculate stddev per country
  {$group: {
      _id: "$Entity",
      nutri_var: {$stdDevPop: {
          $add: ["$Daily_calorie_animal_protein", "$Daily_calorie_vegetal_protein",
                 "$Daily_calorie_fat", "$Daily_calorie_carbohydrates"]
            }}
    }},
  // Join with happiness
  {$lookup: {
      from: "happiness",
      localField: "_id",
      foreignField: "Country",
      as: "h"
    }},
  // Flatten
  {$unwind: "$h"},
  // Add categories (variation & score)
  {$project: {
      country: "$_id",
      nutrition_variation: {$round: ["$nutri_var", 2]},
      happiness_score: "$h.Happiness_Score",
      variation_category: {
        $cond: [{$lt: ["$nutri_var", 100]}, "Low Variation",
          {$cond: [{$lt: ["$nutri_var", 200]}, "Medium Variation", "High Variation"]}]
      },
      score_category: {
        $cond: [{$gte:["$h.Happiness_Score", 7]}, "High Score",
          {$cond: [{$gte: ["$h.Happiness_Score", 5]}, "Medium Score", "Low Score"]}]
      }
  }},
  // Sort
  {$sort: {nutrition_variation: 1}}
])

// ----------------------------------------------------------------
// Q 22

// --------------------------------------------------------------------
// SECTION 1: Identifying Problem Seasons and Nutrients
// --------------------------------------------------------------------

// Aggregate monthly data
db.simulated_food_intake_2015_2020.aggregate([
  {
    $group: {
      _id: {
        year: "$Year",
        month: "$Month"
      },
      avg_fat: { $avg: "$Daily_calorie_fat" },
      avg_carb: { $avg: "$Daily_calorie_carbohydrates" },
      avg_protein: { $avg: { $add: ["$Daily_calorie_animal_protein", "$Daily_calorie_vegetal_protein"] }
      }
    }
  },
  {
    $project: {
      _id: 0,
      year: "$_id.year",
      month: "$_id.month",
      avg_fat: 1,
      avg_carb: 1,
      avg_protein: 1
    }
  },
  { $sort: { year: 1, month: 1 } },
  { $out: "monthly_data" }
]);


db.monthly_data.find({})

// get monthly spikes
db.monthly_data.aggregate([
  {
    $setWindowFields: {
      sortBy: { year: 1, month: 1 },
      output: {
        prev_fat: { $shift: { output: "$avg_fat", by: -1 } },
        prev_carb: { $shift: { output: "$avg_carb", by: -1 } },
        prev_protein: { $shift: { output: "$avg_protein", by: -1 } },
        prev_month: { $shift: { output: "$month", by: -1 } }
      }
    }
  },
  {
    $addFields: {
      month_transition: {
        $concat: [ { $toString: "$prev_month" }, " --> ", { $toString: "$month" } ]
      },
      fat_change: {
        $round: [{ $subtract: ["$avg_fat", "$prev_fat"] }, 2]
      },
      carb_change: {
        $round: [{ $subtract: ["$avg_carb", "$prev_carb"] }, 2]
      },
      protein_change: {
        $round: [{ $subtract: ["$avg_protein", "$prev_protein"] }, 2]
      }
    }
  },
  { $match: { prev_fat: { $exists: true, $ne: null } }
  },
  { $project: {
          _id: 0,
          year: 1,
          month: 1,
          month_transition: 1,
          fat_change: 1,
          carb_change: 1,
          protein_change: 1
    }
  { $out: "changes" }
]);

db.changes.find({})


// Add seasonal patterns
db.changes.aggregate([
  {
    $addFields: {
      season_holiday: {
        $switch: {
          branches: [
            { case: { $in: ["$month", [11, 12]] }, then: "Holiday Season" },
            { case: { $in: ["$month", [1, 2]] }, then: "Post-Holiday" },
            { case: { $in: ["$month", [6, 7, 8]] }, then: "Summer" },
            { case: { $eq: ["$month", 9] }, then: "Back-to-School" },
            { case: { $eq: ["$month", 10] }, then: "Fall" },
            { case: { $in: ["$month", [3, 4, 5]] }, then: "Spring" }
          ],
          default: "NIL"
        }
      }
    }
  },
  {
    $group: {
      _id: {
        month_transition: "$month_transition",
        season_holiday: "$season_holiday"
      },
      avg_fat_change: { $avg: "$fat_change" },
      avg_carb_change: { $avg: "$carb_change" },
      avg_protein_change: { $avg: "$protein_change" }
    }
  },
  {
    $project: {
      _id: 0,
      month: "$_id.month",
      month_transition: "$_id.month_transition",
      season_holiday: "$_id.season_holiday",
      avg_fat_change: { $round: ["$avg_fat_change", 2] },
      avg_carb_change: { $round: ["$avg_carb_change", 2] },
      avg_protein_change: { $round: ["$avg_protein_change", 2] }
    }
  },
  { $out: "seasonal_patterns" }
]);


db.seasonal_patterns.find({})


// Find which nutrient drives each spike
db.seasonal_patterns.aggregate([
  {
    $match: {
      season_holiday: { $in: ["Post-Holiday", "Holiday Season"] },
      avg_fat_change: { $gt: 0 },
      avg_carb_change: { $gt: 0 },
      avg_protein_change: { $gt: 0 }
    }
  },
  { $addFields: { problem_nutrient: { $switch: { branches: [
            {
              case: { $and: [
                  { $gt: ["$avg_fat_change", "$avg_carb_change"] },
                  { $gt: ["$avg_fat_change", "$avg_protein_change"] }
                ]
              }, then: "FAT" },
            {
              case: { $and: [
                  { $gt: ["$avg_carb_change", "$avg_fat_change"] },
                  { $gt: ["$avg_carb_change", "$avg_protein_change"] }
                ]
              }, then: "CARB" }
          ], 
          default: "PROTEIN" } 
        },
      total_change: {
        $add: ["$avg_fat_change", "$avg_carb_change", "$avg_protein_change"]
      }
    }
  },
  { $sort: { total_change: -1 } },
  { $project: {
          _id: 0,
          month_transition: 1,
          season_holiday: 1,
          avg_fat_change: 1,
          avg_carb_change: 1,
          avg_protein_change: 1,
          problem_nutrient: 1
    }
  }
]);

// --------------------------------------------------------------------
// SECTION 2: Identifying Suitable Food Options from Fast Food Chains
// --------------------------------------------------------------------

// Categorise Fast Food
db.fastfood.aggregate([
  {
    $project: {
      restaurant: 1,
      item: 1,
      total_fat: 1,
      total_carb: 1,
      fat_category: {
        $switch: {
          branches: [
            { case: { $gt: ["$total_fat", 43.75] }, then: "HIGH-FAT" },
            { case: { $lte: ["$total_fat", 7.5] }, then: "LOW-FAT" }
          ],
          default: "MODERATE-FAT"
        }
      },
      carb_category: {
        $switch: {
          branches: [
            { case: { $gt: ["$total_carb", 56.25] }, then: "HIGH-CARB" },
            { case: { $lte: ["$total_carb", 12.5] }, then: "LOW-CARB" }
          ],
          default: "MODERATE-CARB"
        }
      }
    }
  },
  { $sort: { total_fat: 1, total_carb: 1 } }
  { $out: "food_category" }
]);

db.food_category.find({})


// Find Healthiest Restaurants
db.food_category.aggregate([
    {
    $match: {
      fat_category: "LOW-FAT",
      carb_category: "LOW-CARB"
        }
    },
  {
    $group: {
      _id: "$restaurant",
      total_items: { $sum: 1 },
      avg_fat: { $avg: "$total_fat" },
      avg_carb: { $avg: "$total_carb" },
      healthy_items_count: {
        $sum: {
          $cond: [
            { $and: [
                { $lte: ["$total_fat", 7.5] },
                { $lte: ["$total_carb", 12.5] }
              ]
            },
            1, 0 ]
        }
      }
    }
  },
  { $addFields: {
      combined_score: { $add: ["$avg_fat", "$avg_carb"] }
    }
  },
  {
    $project: {
      _id: 0,
      restaurant: "$_id",
      total_items: 1,
      avg_fat: { $round: ["$avg_fat", 2] },
      avg_carb: { $round: ["$avg_carb", 2] },
      combined_score: { $round: ["$combined_score", 2] },
      healthy_items_count: 1
    }
  },
  { $sort: { combined_score: 1 } }
]);

// Top 3 low-carb restaurants
db.food_category.aggregate([
    {
    $match: { carb_category: "LOW-CARB" }
    },
  {
    $group: {
      _id: "$restaurant",
      avg_carb: { $avg: "$total_carb" },
      total_items: { $sum: 1 }
    }
  },
  {
    $project: {
      _id: 0,
      restaurant: "$_id",
      avg_carb: { $round: ["$avg_carb", 2] },
      total_items: 1
    }
  },
  { $sort: { avg_carb: 1 } },
  { $limit: 3 },
  { $out: "top3_low_carb_restaurants" }
]);

db.top3_low_carb_restaurants.find({})

// Top 3 low-fat restaurants
db.food_category.aggregate([
    {
    $match: { fat_category: "LOW-FAT" }
    },
  {
    $group: {
      _id: "$restaurant",
      avg_fat: { $avg: "$total_fat" },
      total_items: { $sum: 1 }
    }
  },
  {
    $project: {
      _id: 0,
      restaurant: "$_id",
      avg_fat: { $round: ["$avg_fat", 2] },
      total_items: 1
    }
  },
  { $sort: { avg_fat: 1 } },
  { $limit: 3 },
  { $out: "top3_low_fat_restaurants" }
]);

db.top3_low_fat_restaurants.find({})


// View all partner restaurants and campaign type 
db.top3_low_carb_restaurants.aggregate([
  {
    $project: {
      _id: 0,
      campaign_type: { $literal: "LOW-CARB Partners (Oct-Jan Campaigns)" },
      restaurant: 1,
      avg_nutrient: "$avg_carb"
    }
  },
  {
    $unionWith: {
      coll: "top3_low_fat_restaurants",
      pipeline: [
        {
          $project: {
            _id: 0,
            campaign_type: { $literal: "LOW-FAT Partners (Jan-Feb Campaign)" },
            restaurant: 1,
            avg_nutrient: "$avg_fat"
          }
        }
      ]
    }
  },
  { $out: "campaign_partners" }
]);

db.campaign_partners.find({});



// Get top 5 lowest-carb items for each partner restaurant
db.food_category.aggregate([
  {
    $lookup: {
      from: "top3_low_carb_restaurants",
      localField: "restaurant",
      foreignField: "restaurant",
      as: "is_partner"
    }
  },
  { $match: { is_partner: { $ne: [] } } // This keeps only items from partner restaurant
  },
  { $sort: {
      restaurant: 1,
      total_carb: 1,
    }
  },
  { $group: {
      _id: "$restaurant",
      items: { $push: "$$ROOT" }
    }
  },
  { $project: {
      _id: 0,
      restaurant: "$_id",
      top5_items: { $slice: ["$items", 5] }   // Takes only first 5 items
    }
  },
  { $unwind: {
      path: "$top5_items",
      includeArrayIndex: "rank" // This expands the array
    }
  },
  { $project: {
      campaign_period: { $literal: "CARB-DRIVEN Campaigns (Oct-Jan)" },
      restaurant: 1,
      item: "$top5_items.item",
      total_fat: "$top5_items.total_fat",
      total_carb: "$top5_items.total_carb",
      item_rank: { $add: ["$rank", 1] }
    }
  },
  { $sort: {
      restaurant: 1,
      item_rank: 1
    }
  },
  { $out: "carb_campaign_items" }
]);

db.carb_campaign_items.find({})

// Get top 5 lowest-fat items for each partner restaurant
db.food_category.aggregate([
  {
    $lookup: {
      from: "top3_low_fat_restaurants",
      localField: "restaurant",
      foreignField: "restaurant",
      as: "is_partner"
    }
  },
  { $match: { is_partner: { $ne: [] }
    }
  },
  { $sort: {
      restaurant: 1,
      total_fat: 1,
    }
  },
  { $group: {
      _id: "$restaurant",
      items: { $push: "$$ROOT" }
    }
  },
  { $project: {
      _id: 0,
      restaurant: "$_id",
      top5_items: { $slice: ["$items", 5] }
    }
  },
  { $unwind: {
      path: "$top5_items",
      includeArrayIndex: "rank"
    }
  },
  { $project: {
      campaign_period: { $literal: "FAT-DRIVEN Campaign (Jan-Feb)" },
      restaurant: 1,
      item: "$top5_items.item",
      total_fat: "$top5_items.total_fat",
      total_carb: "$top5_items.total_carb",
      item_rank: { $add: ["$rank", 1] }
    }
  },
  { $sort: {
      restaurant: 1,
      item_rank: 1
    }
  },
  { $out: "fat_campaign_items" }
]);

db.fat_campaign_items.find({});
