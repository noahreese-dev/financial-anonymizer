# Financial Analysis with AI - Implementation Plan

## Overview
This document outlines the planned approach for the "Financial analysis with AI" feature, which will provide intelligent insights and interactive querying of sanitized financial data.

## Current State
- ✅ Data sanitization (PII removal, merchant normalization)
- ✅ Clean output format (Date, Description, Amount)
- ✅ Review Mode for user verification
- ⏸️ AI Analysis (Placeholder button exists, not yet implemented)

## Design Philosophy
**Interactive & Adaptive**: Rather than making assumptions about what the user wants to know, the AI should:
1. First **query the user** about their goals and what types of insights they're seeking
2. Ask clarifying questions about specific transactions or patterns
3. Provide context-aware analysis based on user responses

## Proposed User Flow

### Step 1: Initial Context Gathering
When the user clicks "Financial analysis with AI":
- Modal opens with conversational prompts:
  - "What would you like to understand about your finances?"
  - "Are you tracking spending in specific categories? (e.g., dining, subscriptions)"
  - "Do you have budget goals or spending limits you'd like to monitor?"
  - "Is there a specific time period you're focused on?"

### Step 2: Data Understanding Phase
The AI asks the user to provide context on ambiguous transactions:
- "I see several 'Transfer To [Phone]' entries. Can you help me understand what these represent?"
- "There are charges from 'Tea Hut' - is this a regular expense or one-time purchase?"
- User can label/annotate specific merchants or transaction types

### Step 2.5: Repeating Transaction Pattern Detection

**Automatic Vague Transaction Flagging**

When the AI scans the data, it should proactively identify transactions that meet ALL criteria:
1. **Vague Description**: Generic terms like "Transfer To", "Transfer From", "Payment", or single-word merchants
2. **High Frequency**: Appears 3+ times in the dataset
3. **Inconsistent Amounts**: If amounts vary significantly, it's less likely to be a known subscription

**Example Prompts**:
- "I noticed 'Transfer To [Phone]' appears 8 times with varying amounts between $1-$8. Before I analyze further, could you help me understand what these represent? Are they:
  - Bill payments to a specific service?
  - Person-to-person transfers?
  - Something else?"

- "There are 5 charges from 'Tea Hut' ranging from $4-$10. This could be:
  - A regular coffee/tea habit
  - Social outings
  - Would you like me to track this as a recurring expense category?"

**AI Reasoning Display**:
The AI should briefly explain its thinking:
- "I'm flagging this because the description is generic AND it happens frequently"
- "This pattern suggests a subscription, but the merchant name is unclear"

**User Benefit**:
- Helps identify mystery charges before they get buried in analysis
- Allows user to add context that improves all future insights
- Creates a learning loop where the AI gets smarter about the user's specific finances

### Step 3: Analysis & Insights
Based on user input, the AI generates:
- Spending trends (monthly, by merchant, by pattern)
- Budget vs. actual comparisons (if user provided goals)
- Anomaly detection (unusual spikes or charges)
- Subscription identification and cost analysis
- Cash flow patterns (income timing vs. expense timing)

### Step 4: Interactive Q&A
User can ask follow-up questions:
- "How much did I spend on dining last month?"
- "What are my recurring charges?"
- "Show me all transactions over $100"
- "Compare my spending this month vs. last month"

## Technical Considerations

### Data Format for AI
The clean triad (Date, Description, Amount) is already AI-ready:
```json
[
  { "date": "2025-09-02", "description": "Google Gsuite", "amount": -24.86 },
  { "date": "2025-09-02", "description": "Crunch Fitness", "amount": -13.29 }
]
```

### Privacy & Security
- All processing happens **client-side** until user explicitly sends to AI
- The sanitized data has already stripped PII
- User reviews the output before AI ingestion (Review Mode)
- Clear consent prompt before any external API calls

### AI Integration Options
1. **Option A**: Use a chat interface with the sanitized JSON as context
2. **Option B**: Structured analysis with predefined templates (e.g., "Monthly Summary", "Spending Categories")
3. **Option C**: Hybrid - structured defaults + freeform chat for deep dives

## Categories (Deferred to AI)
We intentionally **do not** auto-categorize transactions in the sanitized output because:
- Our heuristic categorization was producing too many "Other" labels
- The AI model can infer categories more accurately when given the full transaction history
- User input (during the context-gathering phase) provides better categorization than regex rules

The AI can ask:
- "I see charges from 'Adobe' - is this a business expense or personal subscription?"
- "Should I group 'Tim Hortons' and 'Starbucks' into a 'Coffee' category?"

## Next Steps (Post-MVP)
- [ ] Add detail level toggle (Basic / Detailed / Full Debug)
- [ ] Implement conversational AI analysis
- [ ] Add transaction annotation/tagging UI
- [ ] Budget tracking & goal-setting module
- [ ] Export analysis reports (PDF, Markdown summary)

## Notes
- Keep the default output **minimal** (Date, Description, Amount)
- Let the AI model do the heavy lifting for insights
- Prioritize user control and privacy at every step

