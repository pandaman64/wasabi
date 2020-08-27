use id_arena::{Arena, Id};
use std::collections::{BTreeMap, BTreeSet};
use std::collections::{HashMap, HashSet};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Terminal(String);
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Nonterminal(String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Symbol {
    Eof,
    Terminal(Terminal),
    Nonterminal(Nonterminal),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Rule {
    head: SymbolId,
    production: Vec<SymbolId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Item {
    rule: RuleId,
    dot: usize,
}

type SymbolId = Id<Symbol>;
type RuleId = Id<Rule>;
type ItemSet = BTreeSet<Item>;
type ClosureId = Id<ItemSet>;

fn singleton<T: Ord>(item: T) -> BTreeSet<T> {
    let mut items = BTreeSet::new();
    items.insert(item);
    items
}

#[derive(Debug, Clone, Default)]
pub struct LanguageBuilder {
    symbols: Arena<Symbol>,
    rules: Arena<Rule>,
    items: HashSet<Item>,
    heads: HashMap<SymbolId, Vec<RuleId>>,
}

impl LanguageBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_symbol(&mut self, symbol: Symbol) -> SymbolId {
        self.symbols.alloc(symbol)
    }

    pub fn add_rule(&mut self, head: SymbolId, production: Vec<SymbolId>) -> RuleId {
        assert!(matches!(&self.symbols[head], Symbol::Nonterminal(_)));

        let len = production.len();
        let rule = self.rules.alloc(Rule {
            head: head.clone(),
            production,
        });
        self.heads.entry(head).or_default().push(rule);
        for dot in 0..=len {
            self.items.insert(Item { rule, dot });
        }

        rule
    }

    pub fn closure(&self, mut items: ItemSet) -> ItemSet {
        loop {
            let mut new_items = BTreeSet::new();

            for &item in items.iter() {
                let item = &self.items.get(&item).unwrap();
                let rule = &self.rules[item.rule];
                if let Some(sym) = rule.production.get(item.dot) {
                    if let Some(rules) = self.heads.get(sym) {
                        for &rule in rules.iter() {
                            let new_item = Item { rule, dot: 0 };
                            if !items.contains(&new_item) {
                                new_items.insert(new_item);
                            }
                        }
                    }
                }
            }

            if new_items.is_empty() {
                break;
            }
            items.extend(new_items);
        }

        items
    }

    pub fn print_rule(&self, rule: RuleId) -> impl fmt::Display + '_ {
        struct Printer<'a> {
            builder: &'a LanguageBuilder,
            rule: RuleId,
        }

        impl fmt::Display for Printer<'_> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let rule = &self.builder.rules[self.rule];
                let head = &self.builder.symbols[rule.head];

                match head {
                    Symbol::Nonterminal(Nonterminal(nt)) => write!(f, "{} ->", nt)?,
                    _ => unreachable!(),
                }

                for &sym in rule.production.iter() {
                    let sym = &self.builder.symbols[sym];
                    match sym {
                        Symbol::Terminal(t) => write!(f, " \"{}\"", t.0)?,
                        Symbol::Nonterminal(nt) => write!(f, " {}", nt.0)?,
                        Symbol::Eof => write!(f, " $")?,
                    }
                }

                Ok(())
            }
        }

        Printer {
            builder: self,
            rule,
        }
    }

    pub fn print_item(&self, item: Item) -> impl fmt::Display + '_ {
        struct Printer<'a> {
            builder: &'a LanguageBuilder,
            item: Item,
        }

        impl fmt::Display for Printer<'_> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let rule = &self.builder.rules[self.item.rule];
                let head = &self.builder.symbols[rule.head];

                match head {
                    Symbol::Nonterminal(Nonterminal(nt)) => write!(f, "{} ->", nt)?,
                    _ => unreachable!(),
                }

                for (idx, &sym) in rule.production.iter().enumerate() {
                    let sym = &self.builder.symbols[sym];
                    if idx == self.item.dot {
                        write!(f, " .")?;
                    }
                    match sym {
                        Symbol::Terminal(t) => write!(f, " \"{}\"", t.0)?,
                        Symbol::Nonterminal(nt) => write!(f, " {}", nt.0)?,
                        Symbol::Eof => write!(f, " $")?,
                    }
                }
                if self.item.dot == rule.production.len() {
                    write!(f, " .")?;
                }

                Ok(())
            }
        }

        Printer {
            builder: self,
            item,
        }
    }

    pub fn build(&self, initial_rule: Item) -> Language<'_> {
        let mut closures = Arena::new();
        let initial_state;
        // reverse map to closures
        let mut done = HashMap::new();
        let mut table = HashMap::new();

        let mut visit: Vec<ClosureId> = {
            let cls = self.closure(singleton(initial_rule));
            let cls_id = closures.alloc(cls.clone());
            initial_state = cls_id;
            done.insert(cls, cls_id);
            vec![cls_id]
        };

        while !visit.is_empty() {
            let mut new_visit = vec![];
            for from_id in visit.into_iter() {
                let from = closures[from_id].clone();

                let mut transition = HashMap::new();
                for &item in from.iter() {
                    let rule = &self.rules[item.rule];
                    if item.dot < rule.production.len() {
                        let sym = rule.production[item.dot];
                        if !matches!(&self.symbols[sym], Symbol::Eof) {
                            let advanced_item = Item {
                                rule: item.rule,
                                dot: item.dot + 1,
                            };
                            transition
                                .entry(sym)
                                .or_insert_with(ItemSet::new)
                                .insert(advanced_item);
                        }
                    }
                }

                for (sym, items) in transition.into_iter() {
                    let to_cls = self.closure(items);

                    use std::collections::hash_map::Entry;
                    let to_id = match done.entry(to_cls.clone()) {
                        Entry::Occupied(e) => *e.get(),
                        Entry::Vacant(e) => {
                            let to_id = closures.alloc(to_cls);
                            e.insert(to_id);
                            new_visit.push(to_id);
                            to_id
                        }
                    };
                    let old = table
                        .entry(from_id)
                        .or_insert_with(BTreeMap::new)
                        .insert(sym, to_id);
                    assert!(old.is_none());
                }
            }

            visit = new_visit;
        }

        let goto: HashMap<ClosureId, BTreeMap<SymbolId, ClosureId>> = table
            .iter()
            .map(|(&from, tos)| {
                let tos = tos
                    .iter()
                    .filter(|(&sym, _)| matches!(&self.symbols[sym], Symbol::Nonterminal(_)))
                    .map(|(&sym, &to)| (sym, to))
                    .collect();
                (from, tos)
            })
            .collect();
        let mut action: HashMap<ClosureId, BTreeMap<SymbolId, BTreeSet<Action>>> = table
            .iter()
            .map(|(&from, tos)| {
                let tos = tos
                    .iter()
                    .filter(|(&sym, _)| matches!(&self.symbols[sym], Symbol::Terminal(_)))
                    .map(|(&sym, &to)| (sym, singleton(Action::Shift(to))))
                    .collect();
                (from, tos)
            })
            .collect();
        for (id, cls) in closures.iter() {
            for item in cls.iter() {
                let rule = &self.rules[item.rule];
                if item.dot > 0 && item.dot == rule.production.len() {
                    let row = action.entry(id).or_default();
                    for (sym_id, sym) in self.symbols.iter() {
                        if matches!(sym, Symbol::Terminal(_) | Symbol::Eof) {
                            row.entry(sym_id)
                                .or_default()
                                .insert(Action::Reduce(item.rule));
                        }
                    }
                }
                if item.dot < rule.production.len() {
                    let sym = rule.production[item.dot];
                    if matches!(&self.symbols[sym], Symbol::Eof) {
                        action
                            .entry(id)
                            .or_default()
                            .entry(sym)
                            .or_default()
                            .insert(Action::Accept);
                    }
                }
            }
        }

        Language {
            builder: self,
            initial_state,
            closures,
            table,
            goto,
            action,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Action {
    Shift(ClosureId),
    Reduce(RuleId),
    Accept,
}

impl fmt::Debug for Action {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Action::Shift(cls) => write!(f, "s{}", cls.index()),
            Action::Reduce(rule) => write!(f, "r{}", rule.index()),
            Action::Accept => write!(f, "acc"),
        }
    }
}

pub struct Language<'b> {
    builder: &'b LanguageBuilder,
    closures: Arena<ItemSet>,
    initial_state: ClosureId,
    table: HashMap<ClosureId, BTreeMap<SymbolId, ClosureId>>,
    goto: HashMap<ClosureId, BTreeMap<SymbolId, ClosureId>>,
    action: HashMap<ClosureId, BTreeMap<SymbolId, BTreeSet<Action>>>,
}

impl Language<'_> {
    pub fn recognize(&self, mut input: &str) -> Vec<RuleId> {
        let mut stack = vec![self.initial_state];
        let mut output = vec![];

        loop {
            let top = stack.last().unwrap();
            let top_index = top.index();

            // take actions according to the first match of the lexer (comparison against terminal symbols)
            // TODO: add regex lexer, proper ordering (longest match?)
            let mut did_something = false;
            for (&sym, actions) in self.action.get(top).unwrap() {
                assert!(actions.len() <= 1, "GLR is not supported yet");
                let sym = &self.builder.symbols[sym];
                match sym {
                    Symbol::Terminal(t) => {
                        if input.starts_with(&t.0) {
                            match &actions.iter().next() {
                                Some(Action::Shift(next)) => {
                                    input = &input[t.0.len()..];
                                    stack.push(*next)
                                }
                                Some(Action::Reduce(rule)) => {
                                    output.push(*rule);
                                    let rule = &self.builder.rules[*rule];

                                    assert!(
                                        stack.len() > rule.production.len(),
                                        "not enough states in stack"
                                    );
                                    for _ in 0..rule.production.len() {
                                        stack.pop().unwrap();
                                    }

                                    let top = stack.last().unwrap();
                                    let goto = self.goto.get(top).unwrap();
                                    stack.push(*goto.get(&rule.head).unwrap());
                                }
                                Some(Action::Accept) => {
                                    unreachable!("terminal columns cannot have accept")
                                }
                                None => unreachable!("there must be an action"),
                            }

                            did_something = true;
                            break;
                        }
                    }
                    Symbol::Eof => {
                        if input.is_empty() {
                            match &actions.iter().next() {
                                Some(Action::Accept) => return output,
                                Some(Action::Reduce(rule)) => {
                                    output.push(*rule);
                                    let rule = &self.builder.rules[*rule];

                                    assert!(
                                        stack.len() > rule.production.len(),
                                        "not enough states in stack"
                                    );
                                    for _ in 0..rule.production.len() {
                                        stack.pop().unwrap();
                                    }

                                    let top = stack.last().unwrap();
                                    let goto = self.goto.get(top).unwrap();
                                    stack.push(*goto.get(&rule.head).unwrap());

                                    did_something = true;
                                }
                                _ => unreachable!("EOF column can have only reduce/accept actions"),
                            }
                        }
                    }
                    _ => unreachable!("must be a terminal/EOF"),
                }
            }

            // TODO: proper error handling
            if !did_something {
                panic!(
                    "no rule found for state {}, input = \"{}\"",
                    top_index, input
                );
            }
        }
    }
}

impl fmt::Display for Language<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "Language with initial state {}",
            self.initial_state.index()
        )?;
        for (id, cls) in self.closures.iter() {
            writeln!(f, "= {:3} ================", id.index())?;
            for &item in cls.iter() {
                writeln!(f, "{}", self.builder.print_item(item))?;
            }
            writeln!(f, "transitions:")?;
            if let Some(edges) = self.table.get(&id) {
                for (&sym, to) in edges.iter() {
                    match &self.builder.symbols[sym] {
                        Symbol::Terminal(t) => writeln!(f, "\"{}\" -> {}", t.0, to.index())?,
                        Symbol::Nonterminal(nt) => writeln!(f, " {}  -> {}", nt.0, to.index())?,
                        Symbol::Eof => writeln!(f, " $  -> {}", to.index())?,
                    }
                }
            }
            writeln!(f, "goto:")?;
            if let Some(edges) = self.goto.get(&id) {
                for (&sym, to) in edges.iter() {
                    match &self.builder.symbols[sym] {
                        Symbol::Terminal(t) => writeln!(f, "\"{}\" -> {}", t.0, to.index())?,
                        Symbol::Nonterminal(nt) => writeln!(f, " {}  -> {}", nt.0, to.index())?,
                        Symbol::Eof => writeln!(f, " $  -> {}", to.index())?,
                    }
                }
            }
            writeln!(f, "actions:")?;
            if let Some(edges) = self.action.get(&id) {
                for (&sym, actions) in edges.iter() {
                    struct Printer<'a> {
                        actions: &'a BTreeSet<Action>,
                    }
                    impl fmt::Display for Printer<'_> {
                        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                            f.debug_list().entries(self.actions.iter()).finish()
                        }
                    }
                    let printer = Printer { actions };
                    match &self.builder.symbols[sym] {
                        Symbol::Terminal(t) => writeln!(f, "\"{}\" -> {}", t.0, printer)?,
                        Symbol::Nonterminal(nt) => writeln!(f, " {}  -> {}", nt.0, printer)?,
                        Symbol::Eof => writeln!(f, " $  -> {}", printer)?,
                    }
                }
            }
        }

        Ok(())
    }
}

#[test]
fn test_closure() {
    let mut builder = LanguageBuilder::new();
    let eof = builder.add_symbol(Symbol::Eof);
    let zero = builder.add_symbol(Symbol::Terminal(Terminal("0".into())));
    let one = builder.add_symbol(Symbol::Terminal(Terminal("1".into())));
    let plus = builder.add_symbol(Symbol::Terminal(Terminal("+".into())));
    let mult = builder.add_symbol(Symbol::Terminal(Terminal("*".into())));
    let lit = builder.add_symbol(Symbol::Nonterminal(Nonterminal("B".into())));
    let expr = builder.add_symbol(Symbol::Nonterminal(Nonterminal("E".into())));
    let prog = builder.add_symbol(Symbol::Nonterminal(Nonterminal("S".into())));

    builder.add_rule(lit, vec![zero]);
    builder.add_rule(lit, vec![one]);
    builder.add_rule(expr, vec![lit]);
    builder.add_rule(expr, vec![expr, plus, lit]);
    builder.add_rule(expr, vec![expr, mult, lit]);
    let prog = builder.add_rule(prog, vec![expr, eof]);

    let lang = builder.build(Item { rule: prog, dot: 0 });
    println!("{}", lang);

    for (rule, _) in builder.rules.iter() {
        println!("{:2} {}", rule.index(), builder.print_rule(rule));
    }
    println!("{:?}", lang.recognize("0*1+1+1*1"));
}
