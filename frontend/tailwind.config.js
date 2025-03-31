// tailwind.config.js
/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/**/*.{js,jsx,ts,tsx}", // Adjust paths as needed
  ],
  theme: {
    extend: {
      colors: {
        // More subdued coffee & maid cafe palette
        'maid-pink': { 
          light: '#f8e8e8', // Very subtle pink (almost cream)
          DEFAULT: '#e8b7b7', // Muted rose pink
          dark: '#d69999', // Deeper muted rose
        },
        'maid-cream': { 
          light: '#fcfaf7', // Warm white
          DEFAULT: '#f5f1e8', // Latte cream
          dark: '#e6dfd0', // Slightly darker cream
        },
        'maid-blue': { 
          light: '#edf2f7', // Very light blue-gray
          DEFAULT: '#cbd5e0', // Soft blue-gray
          dark: '#a0aec0', // Deeper blue-gray
        },
        'maid-choco': { 
          light: '#8c7851', // Light brown
          DEFAULT: '#6b5c41', // Medium coffee brown
          dark: '#4a3f2d', // Dark coffee
        },
        'maid-gray': { 
          light: '#f0f0f0', // Light gray
          DEFAULT: '#d1d1d1', // Medium gray
          dark: '#9e9e9e', // Darker gray
        },
        'coffee': {
          light: '#c7b198', // Light coffee
          DEFAULT: '#a58d72', // Medium coffee
          dark: '#7e685a', // Dark coffee
        }
      },
      fontFamily: {
        // Use a clean, slightly rounded sans-serif
        sans: ['Nunito', 'Segoe UI', 'Roboto', 'Helvetica Neue', 'Arial', 'sans-serif'],
      },
      boxShadow: {
        'soft': '0 2px 8px rgba(0, 0, 0, 0.04)',
        'soft-lg': '0 4px 12px rgba(0, 0, 0, 0.06)',
      },
      borderRadius: {
        'xl': '1rem', // Softer large rounding
        'lg': '0.6rem', // Softer default rounding
        'md': '0.4rem',
      }
    },
  },
  plugins: [],
}