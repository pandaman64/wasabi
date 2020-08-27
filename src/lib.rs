use id_arena::{Arena, Id};
use std::collections::{BTreeMap, BTreeSet};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Terminal(String);
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Nonterminal(String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Symbol {
    Terminal(Terminal),
    Nonterminal(Nonterminal),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Rule {
    head: Nonterminal,
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

fn singleton(item: Item) -> ItemSet {
    let mut items = ItemSet::new();
    items.insert(item);
    items
}

#[derive(Debug, Default)]
pub struct LanguageBuilder {
    symbols: Arena<Symbol>,
    rules: Arena<Rule>,
    items: HashSet<Item>,
    heads: HashMap<Nonterminal, Vec<RuleId>>,
}

impl LanguageBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_symbol(&mut self, symbol: Symbol) -> SymbolId {
        self.symbols.alloc(symbol)
    }

    pub fn add_rule(&mut self, head: Nonterminal, production: Vec<SymbolId>) -> RuleId {
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
                if let Some(&sym) = rule.production.get(item.dot) {
                    if let Symbol::Nonterminal(nt) = &self.symbols[sym] {
                        if let Some(rules) = self.heads.get(nt) {
                            for &rule in rules.iter() {
                                let new_item = Item { rule, dot: 0 };
                                if !items.contains(&new_item) {
                                    new_items.insert(new_item);
                                }
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

    pub fn pretty_print(&self, item: Item) -> impl std::fmt::Display + '_ {
        struct ItemPrinter<'a> {
            builder: &'a LanguageBuilder,
            item: Item,
        }

        impl std::fmt::Display for ItemPrinter<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                let rule = &self.builder.rules[self.item.rule];
                write!(f, "{} ->", rule.head.0)?;
                for (idx, &sym) in rule.production.iter().enumerate() {
                    let sym = &self.builder.symbols[sym];
                    if idx == self.item.dot {
                        write!(f, " .")?;
                    }
                    match sym {
                        Symbol::Terminal(t) => write!(f, " \"{}\"", t.0)?,
                        Symbol::Nonterminal(nt) => write!(f, " {}", nt.0)?,
                    }
                }
                if self.item.dot == rule.production.len() {
                    write!(f, " .")?;
                }

                Ok(())
            }
        }

        ItemPrinter {
            builder: self,
            item,
        }
    }

    pub fn build_table(
        &self,
        starting_rule: Item,
    ) -> (
        Arena<ItemSet>,
        HashMap<ClosureId, BTreeMap<SymbolId, ClosureId>>,
    ) {
        let mut closures = Arena::new();
        // reverse map to closures
        let mut done = HashMap::new();
        let mut table = HashMap::new();

        let mut visit: Vec<ClosureId> = {
            let cls = self.closure(singleton(starting_rule));
            let cls_id = closures.alloc(cls.clone());
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

        (closures, table)
    }
}

#[test]
fn test_closure() {
    let mut builder = LanguageBuilder::new();
    let zero = builder.add_symbol(Symbol::Terminal(Terminal("0".into())));
    let one = builder.add_symbol(Symbol::Terminal(Terminal("1".into())));
    let plus = builder.add_symbol(Symbol::Terminal(Terminal("+".into())));
    let mult = builder.add_symbol(Symbol::Terminal(Terminal("*".into())));
    let lit = Nonterminal("B".into());
    let expr = Nonterminal("E".into());
    let prog = Nonterminal("S".into());
    let lit_sym = builder.add_symbol(Symbol::Nonterminal(lit.clone()));
    let expr_sym = builder.add_symbol(Symbol::Nonterminal(expr.clone()));
    let _prog_sym = builder.add_symbol(Symbol::Nonterminal(prog.clone()));

    builder.add_rule(lit.clone(), vec![zero]);
    builder.add_rule(lit.clone(), vec![one]);
    builder.add_rule(expr.clone(), vec![lit_sym]);
    builder.add_rule(expr.clone(), vec![expr_sym, plus, lit_sym]);
    builder.add_rule(expr.clone(), vec![expr_sym, mult, lit_sym]);
    let prog = builder.add_rule(prog.clone(), vec![expr_sym]);

    let (closures, table) = builder.build_table(Item { rule: prog, dot: 0 });

    for (id, cls) in closures.iter() {
        println!("={:3}=================", id.index());
        for &item in cls.iter() {
            println!("{}", builder.pretty_print(item));
        }
        println!();
        if let Some(edges) = table.get(&id) {
            for (&sym, to) in edges.iter() {
                match &builder.symbols[sym] {
                    Symbol::Terminal(t) => println!("\"{}\" -> {}", t.0, to.index()),
                    Symbol::Nonterminal(nt) => println!(" {}  -> {}", nt.0, to.index()),
                }
            }
        }
    }
}
