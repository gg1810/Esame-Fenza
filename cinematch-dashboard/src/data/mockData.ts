// ============================================
// CINEMATCH - MOCK DATA
// ============================================

export interface Movie {
  id: number;
  title: string;
  year: number;
  poster: string;
  rating: number;
  genres: string[];
  director: string;
  matchScore?: number;
  sentiment?: 'positive' | 'neutral' | 'negative';
  boxOffice?: number;
  predictedBoxOffice?: number;
}

export interface UserStats {
  filmsWatched: number;
  averageRating: number;
  favoriteGenres: number;
  watchTime: number;
  totalReviews: number;
  watchlistSize: number;
}

export interface MonthlyData {
  month: string;
  films: number;
  hours: number;
}

export interface GenreData {
  name: string;
  value: number;
  color: string;
}

export interface SentimentPost {
  id: number;
  title: string;
  subreddit: string;
  score: number;
  sentiment: 'positive' | 'neutral' | 'negative';
  sentimentScore: number;
  date: string;
}

export interface CinemaMovie extends Movie {
  showtimes: string[];
  theater: string;
  redditSentiment: number;
}

// Mock User Stats
export const userStats: UserStats = {
  filmsWatched: 247,
  averageRating: 3.8,
  favoriteGenres: 5,
  watchTime: 412,
  totalReviews: 189,
  watchlistSize: 67
};

// Monthly viewing data
export const monthlyData: MonthlyData[] = [
  { month: 'Gen', films: 12, hours: 24 },
  { month: 'Feb', films: 8, hours: 16 },
  { month: 'Mar', films: 15, hours: 30 },
  { month: 'Apr', films: 10, hours: 20 },
  { month: 'Mag', films: 18, hours: 36 },
  { month: 'Giu', films: 14, hours: 28 },
  { month: 'Lug', films: 22, hours: 44 },
  { month: 'Ago', films: 20, hours: 40 },
  { month: 'Set', films: 16, hours: 32 },
  { month: 'Ott', films: 19, hours: 38 },
  { month: 'Nov', films: 25, hours: 50 },
  { month: 'Dic', films: 21, hours: 42 }
];

// Genre distribution
export const genreData: GenreData[] = [
  { name: 'Drama', value: 35, color: '#E50914' },
  { name: 'Thriller', value: 20, color: '#FF6B35' },
  { name: 'Sci-Fi', value: 15, color: '#00529B' },
  { name: 'Comedy', value: 12, color: '#8B9A7D' },
  { name: 'Horror', value: 10, color: '#0033A0' },
  { name: 'Action', value: 8, color: '#C5A572' }
];

// Recommended movies
export const recommendedMovies: Movie[] = [
  {
    id: 1,
    title: 'Past Lives',
    year: 2023,
    poster: 'https://image.tmdb.org/t/p/w500/k3waqVXSnvCZWfJYNtdamTgTtTA.jpg',
    rating: 4.2,
    genres: ['Drama', 'Romance'],
    director: 'Celine Song',
    matchScore: 95
  },
  {
    id: 2,
    title: 'Aftersun',
    year: 2022,
    poster: 'https://image.tmdb.org/t/p/w500/aHLFr5k2vheVVaOzUPnMlA2Sjwq.jpg',
    rating: 4.5,
    genres: ['Drama'],
    director: 'Charlotte Wells',
    matchScore: 92
  },
  {
    id: 3,
    title: 'The Whale',
    year: 2022,
    poster: 'https://image.tmdb.org/t/p/w500/jQ0gylJMxWSL490sy0RrPj1Lj7e.jpg',
    rating: 4.0,
    genres: ['Drama'],
    director: 'Darren Aronofsky',
    matchScore: 88
  },
  {
    id: 4,
    title: 'Everything Everywhere All at Once',
    year: 2022,
    poster: 'https://image.tmdb.org/t/p/w500/w3LxiVYdWWRvEVdn5RYq6jIqkb1.jpg',
    rating: 4.7,
    genres: ['Sci-Fi', 'Action', 'Comedy'],
    director: 'Daniel Kwan, Daniel Scheinert',
    matchScore: 97
  },
  {
    id: 5,
    title: 'The Banshees of Inisherin',
    year: 2022,
    poster: 'https://image.tmdb.org/t/p/w500/4yFG6cSPaCaPhyJ1vtGOtMBcDWd.jpg',
    rating: 4.3,
    genres: ['Drama', 'Comedy'],
    director: 'Martin McDonagh',
    matchScore: 91
  },
  {
    id: 6,
    title: 'Poor Things',
    year: 2023,
    poster: 'https://image.tmdb.org/t/p/w500/kCGlIMHnOm8JPXq3rXM6c5wMxcT.jpg',
    rating: 4.4,
    genres: ['Sci-Fi', 'Comedy', 'Romance'],
    director: 'Yorgos Lanthimos',
    matchScore: 89
  }
];

// Not recommended movies
export const notRecommendedMovies: Movie[] = [
  {
    id: 7,
    title: 'Transformers: Rise of the Beasts',
    year: 2023,
    poster: 'https://image.tmdb.org/t/p/w500/gPbM0MK8CP8A174rmUwGsADNYKD.jpg',
    rating: 2.5,
    genres: ['Action', 'Sci-Fi'],
    director: 'Steven Caple Jr.',
    matchScore: 23
  },
  {
    id: 8,
    title: 'Fast X',
    year: 2023,
    poster: 'https://image.tmdb.org/t/p/w500/fiVW06jE7z9YnO4trhaMEdclSiC.jpg',
    rating: 2.8,
    genres: ['Action'],
    director: 'Louis Leterrier',
    matchScore: 31
  },
  {
    id: 9,
    title: 'Aquaman and the Lost Kingdom',
    year: 2023,
    poster: 'https://image.tmdb.org/t/p/w500/7lTnXOy0iNtBAdRP3TZvaKJ77F6.jpg',
    rating: 2.6,
    genres: ['Action', 'Fantasy'],
    director: 'James Wan',
    matchScore: 28
  }
];

// Cinema movies (The Space)
export const cinemaMovies: CinemaMovie[] = [
  {
    id: 10,
    title: 'Dune: Part Two',
    year: 2024,
    poster: 'https://image.tmdb.org/t/p/w500/8b8R8l88Qje9dn9OE8PY05Nxl1X.jpg',
    rating: 4.6,
    genres: ['Sci-Fi', 'Adventure'],
    director: 'Denis Villeneuve',
    showtimes: ['15:30', '18:00', '21:00'],
    theater: 'The Space Cinema',
    redditSentiment: 87
  },
  {
    id: 11,
    title: 'Oppenheimer',
    year: 2023,
    poster: 'https://image.tmdb.org/t/p/w500/8Gxv8gSFCU0XGDykEGv7zR1n2ua.jpg',
    rating: 4.8,
    genres: ['Drama', 'History'],
    director: 'Christopher Nolan',
    showtimes: ['14:00', '17:30', '21:00'],
    theater: 'The Space Cinema',
    redditSentiment: 92
  },
  {
    id: 12,
    title: 'Killers of the Flower Moon',
    year: 2023,
    poster: 'https://image.tmdb.org/t/p/w500/dB6Krk806zeqd0YNp2ngQ9zXteH.jpg',
    rating: 4.5,
    genres: ['Crime', 'Drama', 'History'],
    director: 'Martin Scorsese',
    showtimes: ['16:00', '20:30'],
    theater: 'The Space Cinema',
    redditSentiment: 85
  }
];

// Reddit sentiment posts
export const sentimentPosts: SentimentPost[] = [
  {
    id: 1,
    title: "Just watched Dune 2, absolutely blown away by the visuals!",
    subreddit: 'r/movies',
    score: 15420,
    sentiment: 'positive',
    sentimentScore: 0.92,
    date: '2024-03-02'
  },
  {
    id: 2,
    title: "Dune Part Two is the best sci-fi movie of the decade",
    subreddit: 'r/dune',
    score: 8930,
    sentiment: 'positive',
    sentimentScore: 0.88,
    date: '2024-03-01'
  },
  {
    id: 3,
    title: "The Hans Zimmer soundtrack in Dune 2 deserves an Oscar",
    subreddit: 'r/movies',
    score: 6750,
    sentiment: 'positive',
    sentimentScore: 0.85,
    date: '2024-03-03'
  },
  {
    id: 4,
    title: "Anyone else think Dune 2 was too long?",
    subreddit: 'r/TrueFilm',
    score: 234,
    sentiment: 'neutral',
    sentimentScore: 0.45,
    date: '2024-03-02'
  },
  {
    id: 5,
    title: "Disappointed by the ending of Dune 2",
    subreddit: 'r/movies',
    score: 156,
    sentiment: 'negative',
    sentimentScore: 0.28,
    date: '2024-03-04'
  }
];

// Mood types
export const moods = [
  { id: 'happy', emoji: '😊', label: 'Felice', color: '#FFD93D' },
  { id: 'sad', emoji: '😢', label: 'Malinconico', color: '#6C9BCF' },
  { id: 'excited', emoji: '🤩', label: 'Eccitato', color: '#FF6B35' },
  { id: 'relaxed', emoji: '😌', label: 'Rilassato', color: '#8B9A7D' },
  { id: 'romantic', emoji: '💕', label: 'Romantico', color: '#FF69B4' },
  { id: 'thrilling', emoji: '😱', label: 'Thriller', color: '#E50914' }
];

// Mood-based recommendations
export const moodMovies: Record<string, Movie[]> = {
  happy: [
    { id: 20, title: 'The Grand Budapest Hotel', year: 2014, poster: 'https://image.tmdb.org/t/p/w500/eWdyYQreja6JGCzqHWXpWHDrrPo.jpg', rating: 4.5, genres: ['Comedy', 'Drama'], director: 'Wes Anderson' },
    { id: 21, title: 'Paddington 2', year: 2017, poster: 'https://image.tmdb.org/t/p/w500/aHFIbztxdEHHHXlMNfNrIrMMvqE.jpg', rating: 4.6, genres: ['Comedy', 'Family'], director: 'Paul King' },
  ],
  sad: [
    { id: 22, title: 'Manchester by the Sea', year: 2016, poster: 'https://image.tmdb.org/t/p/w500/Af4gRhqPe2KIlTOdqRy9I4xJq5.jpg', rating: 4.3, genres: ['Drama'], director: 'Kenneth Lonergan' },
    { id: 23, title: 'A Ghost Story', year: 2017, poster: 'https://image.tmdb.org/t/p/w500/krB2kCNmNDgBcOsFQgCr2ybAefZ.jpg', rating: 4.1, genres: ['Drama', 'Fantasy'], director: 'David Lowery' },
  ],
  excited: [
    { id: 24, title: 'Mad Max: Fury Road', year: 2015, poster: 'https://image.tmdb.org/t/p/w500/8tZYtuWezp8JbcsvHYO0O46tFbo.jpg', rating: 4.7, genres: ['Action', 'Sci-Fi'], director: 'George Miller' },
    { id: 25, title: 'Top Gun: Maverick', year: 2022, poster: 'https://image.tmdb.org/t/p/w500/62HCnUTziyWcpDaBO2i1DX17ljH.jpg', rating: 4.5, genres: ['Action', 'Drama'], director: 'Joseph Kosinski' },
  ],
  relaxed: [
    { id: 26, title: 'Lost in Translation', year: 2003, poster: 'https://image.tmdb.org/t/p/w500/AvKJNarBQPqHMy8WGijp9Fqwo.jpg', rating: 4.2, genres: ['Drama', 'Romance'], director: 'Sofia Coppola' },
    { id: 27, title: 'The Secret Life of Walter Mitty', year: 2013, poster: 'https://image.tmdb.org/t/p/w500/tRMQXWL0fAU5rPYxOdvx7gpG5Q1.jpg', rating: 4.0, genres: ['Adventure', 'Comedy'], director: 'Ben Stiller' },
  ],
  romantic: [
    { id: 28, title: 'La La Land', year: 2016, poster: 'https://image.tmdb.org/t/p/w500/uDO8zWDhfWwoFdKS4fzkUJt0Rf0.jpg', rating: 4.4, genres: ['Musical', 'Romance'], director: 'Damien Chazelle' },
    { id: 29, title: 'Before Sunrise', year: 1995, poster: 'https://image.tmdb.org/t/p/w500/8zRUrUaoJjMUV3IaLnq7HWNHUPT.jpg', rating: 4.5, genres: ['Drama', 'Romance'], director: 'Richard Linklater' },
  ],
  thrilling: [
    { id: 30, title: 'Sicario', year: 2015, poster: 'https://image.tmdb.org/t/p/w500/l8s2rE8JOVNx8PBXH4EEr1WBfPW.jpg', rating: 4.4, genres: ['Action', 'Thriller'], director: 'Denis Villeneuve' },
    { id: 31, title: 'Prisoners', year: 2013, poster: 'https://image.tmdb.org/t/p/w500/uhvJoGVkuLGy0G0kPBvV8I3LGkO.jpg', rating: 4.5, genres: ['Crime', 'Thriller'], director: 'Denis Villeneuve' },
  ]
};

// Box office predictions
export const boxOfficePredictions: Movie[] = [
  {
    id: 40,
    title: 'Deadpool 3',
    year: 2024,
    poster: 'https://image.tmdb.org/t/p/w500/8cdWjvZQUExUUTzyp4t6EDMubfO.jpg',
    rating: 0,
    genres: ['Action', 'Comedy'],
    director: 'Shawn Levy',
    boxOffice: 0,
    predictedBoxOffice: 890000000
  },
  {
    id: 41,
    title: 'Joker: Folie à Deux',
    year: 2024,
    poster: 'https://image.tmdb.org/t/p/w500/aciP8Km0wgPJQpQj3f3jGIqKrIZ.jpg',
    rating: 0,
    genres: ['Crime', 'Drama', 'Musical'],
    director: 'Todd Phillips',
    boxOffice: 0,
    predictedBoxOffice: 650000000
  },
  {
    id: 42,
    title: 'Gladiator II',
    year: 2024,
    poster: 'https://image.tmdb.org/t/p/w500/2cxhvwyEwRlysAmRH4iodkvo0z5.jpg',
    rating: 0,
    genres: ['Action', 'Drama'],
    director: 'Ridley Scott',
    boxOffice: 0,
    predictedBoxOffice: 520000000
  }
];

// Top directors
export const topDirectors = [
  { name: 'Denis Villeneuve', films: 8, avgRating: 4.5 },
  { name: 'Christopher Nolan', films: 11, avgRating: 4.4 },
  { name: 'Martin Scorsese', films: 7, avgRating: 4.3 },
  { name: 'David Fincher', films: 6, avgRating: 4.2 },
  { name: 'Greta Gerwig', films: 4, avgRating: 4.1 }
];
