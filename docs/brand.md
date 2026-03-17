# DBPort Brand

## Core idea
**DBPort is the trusted harbor for DuckDB data products.**  
The identity should feel calm, precise, reliable, and developer-native.

## Brand traits
- Reliable
- Professional
- Calm
- Precise
- Open-source friendly

Avoid:
- flashy startup aesthetics
- overly corporate tone
- playful nautical themes
- heavy visual complexity

## Logo
Use the **Lucide fishing-hook** icon as the mark.

Preferred lockup:
- hook icon + `DBPort` wordmark

Guidelines:
- keep the icon simple and monochrome or two-tone
- use clean geometric line styling
- avoid gradients, shadows, and decorative effects
- pair with a straightforward sans-serif wordmark

## Color palette
### Primary
- **Harbor Blue** `#1F4E79` — main brand color

### Supporting blues
- **Deep Current** `#16354F` — dark anchor/background
- **Tide Blue** `#4F7EA8` — secondary blue/highlight

### Neutrals
- **Steel Slate** `#2A3440` — primary text
- **Fog Gray** `#66727F` — secondary text/borders
- **Mist** `#EEF3F7` — panels/code backgrounds
- **White** `#FFFFFF` — main background

### Accent
- **Signal Coral** `#E07A5F` — sparing highlight/emphasis only

### Semantic colors
- **Success** `#2F7D5C`
- **Warning** `#C58A1C`
- **Error** `#B44949`
- **Info** `#2F6FA3`

## Usage rules
- Blue is the brand anchor; use Harbor Blue most often.
- Deep Current is for dark surfaces, nav, hero areas, and strong emphasis.
- Neutrals should carry most of the interface.
- Signal Coral should be used rarely for focus or highlights, never as a second primary.

## Typography
### Sans-serif
- **Inter**

Use for:
- website
- docs
- UI
- wordmark
- diagrams

### Monospace
- **JetBrains Mono**

Use for:
- code
- CLI commands
- config examples
- technical labels

## Typography rules
- Headings: Inter SemiBold/Bold
- Body: Inter Regular
- Code: JetBrains Mono
- Keep spacing open and readable
- Prefer clarity over density

## Visual style
- Clean, structured, and technical
- Slightly rounded corners
- Thin, precise dividers and icon lines
- Light backgrounds by default
- Strong contrast, minimal ornament

The look should be maritime-adjacent, not themed.

## UI guidance
### Light mode
Default presentation:
- background: white
- text: Steel Slate
- links/actions: Harbor Blue
- panels/code: Mist

### Dark mode
- background: `#0F1C28`
- panels: Deep Current
- text: `#EAF1F6`
- muted text: `#A7B7C6`

Avoid neon or highly saturated dark themes.

## Documentation tone
The docs should look:
- airy
- trustworthy
- code-native
- calm and structured

The writing should be:
- clear
- precise
- modestly confident
- practical

## Recommended taglines
- **Build locally. Publish safely.**
- **The production layer for DuckDB data products.**
- **Governed inputs. Flexible models. Reliable outputs.**

## Design tokens
```css
:root {
  --dbport-color-primary: #1F4E79;
  --dbport-color-primary-dark: #16354F;
  --dbport-color-primary-light: #4F7EA8;

  --dbport-color-text: #2A3440;
  --dbport-color-text-muted: #66727F;
  --dbport-color-bg: #FFFFFF;
  --dbport-color-bg-soft: #EEF3F7;

  --dbport-color-accent: #E07A5F;

  --dbport-color-success: #2F7D5C;
  --dbport-color-warning: #C58A1C;
  --dbport-color-error: #B44949;
  --dbport-color-info: #2F6FA3;

  --dbport-font-sans: "Inter", system-ui, sans-serif;
  --dbport-font-mono: "JetBrains Mono", ui-monospace, monospace;
}
```

## One-line positioning
**DBPort is the trusted harbor for DuckDB data products: calm, precise, and production-ready.**
